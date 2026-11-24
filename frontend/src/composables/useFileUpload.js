import { ref } from "vue";
import {
  completeFileUpload,
  getPartSize,
  initFileUpload,
  presignMultipartParts,
  uploadMultipartPartToStorage,
} from "../services/http/api.js";
import { UploadStatus } from "../constants/upload.js";

const DEFAULT_CONCURRENCY = 3;
const DEFAULT_MAX_FILE_SIZE = 1024 * 1024 * 1024;
const DEFAULT_MAX_HASH_FILE_SIZE = 512 * 1024 * 1024;
const HASH_MEMORY_MULTIPLIER = 2;
const DEVICE_HASH_MEMORY_RATIO = 0.25;
const MAX_RETRIES = 3;
const MAX_COMPLETE_REPAIR_ROUNDS = 2;

export function useFileUpload(options = {}) {
  const concurrency = options.concurrency || DEFAULT_CONCURRENCY;
  const maxFileSize = options.maxFileSize || DEFAULT_MAX_FILE_SIZE;
  const maxHashFileSize = options.maxHashFileSize || DEFAULT_MAX_HASH_FILE_SIZE;
  const status = ref(UploadStatus.IDLE);
  const progress = ref(0);
  const paused = ref(false);
  const canceled = ref(false);
  // 分片规格由服务端下发（OpenIM PartSize），不再用本地常量切片
  let partSize = 0;
  // 收集所有的上传任务，当取消时，取消上传中的任务
  const activeRequests = new Set();
  let releasePause = null;
  const reset = () => {
    activeRequests.forEach((controller) => controller.abort());
    activeRequests.clear();
    status.value = UploadStatus.IDLE;
    progress.value = 0;
    paused.value = false;
    canceled.value = false;
    releasePause?.();
    releasePause = null;
    resumePromise = null;
  };

  const updateMultipartProgress = (completedParts, totalChunks) => {
    progress.value =
      totalChunks > 0 ? Math.min(100, Math.floor((completedParts.size / totalChunks) * 100)) : 0;
  };

  let resumePromise = null;

  const waitUntilResumed = () => {
    if (!paused.value) return Promise.resolve();

    if (!resumePromise) {
      resumePromise = new Promise((resolve) => {
        releasePause = () => {
          paused.value = false;
          resumePromise = null;
          releasePause = null;
          resolve();
        };
      });
    }

    return resumePromise;
  };

  const uploadPartWithRetry = async (
    file,
    currentUploadId,
    partNumber,
    partURLs,
    completedParts,
    completedEtags,
    totalChunks,
  ) => {
    const start = (partNumber - 1) * partSize;
    const chunk = file.slice(start, Math.min(file.size, start + partSize));
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
      try {
        const partURL = partURLs.get(partNumber);
        if (!partURL) throw new Error("缺少分片上传地址");

        const controller = new AbortController();
        activeRequests.add(controller);
        try {
          const response = await uploadMultipartPartToStorage(partURL, chunk, controller.signal);
          // MinIO/S3 每片 PUT 成功后在响应头返回 ETag，complete 阶段需回传
          const etag = response?.headers?.etag || response?.headers?.ETag;
          if (etag) completedEtags.set(partNumber, etag.replace(/"/g, ""));
        } finally {
          activeRequests.delete(controller);
        }

        completedParts.add(partNumber);
        updateMultipartProgress(completedParts, totalChunks);
        return;
      } catch (uploadError) {
        if (
          canceled.value ||
          uploadError?.name === "CanceledError" ||
          uploadError?.name === "AbortError" ||
          uploadError?.code === "ERR_CANCELED"
        )
          throw uploadError;

        if (attempt === MAX_RETRIES) throw uploadError;
        try {
          const refreshed = await presignMultipartParts(currentUploadId, [partNumber]);
          if (refreshed[0]) partURLs.set(partNumber, refreshed[0].url);
        } catch {
          // 保留原上传错误，下一次循环仍会按重试策略处理。
        }
        await sleep(500 * attempt);
      }
    }
  };

  const presignParts = async (currentUploadId, partNumbers) => {
    const partURLs = new Map();
    for (let offset = 0; offset < partNumbers.length; offset += 100) {
      const batch = await presignMultipartParts(
        currentUploadId,
        partNumbers.slice(offset, offset + 100),
      );
      batch.forEach((part) => partURLs.set(part.partNumber, part.url));
    }
    return partURLs;
  };

  const uploadParts = async (
    file,
    currentUploadId,
    partNumbers,
    completedParts,
    completedEtags,
    totalChunks,
  ) => {
    if (!partNumbers.length) return;
    for (const partNumber of partNumbers) {
      completedParts.delete(partNumber);
    }
    updateMultipartProgress(completedParts, totalChunks);
    const partURLs = await presignParts(currentUploadId, partNumbers);
    let cursor = 0;
    const worker = async () => {
      while (cursor < partNumbers.length) {
        if (canceled.value) throw new Error("上传已取消");
        await waitUntilResumed();
        if (canceled.value) throw new Error("上传已取消");
        if (cursor >= partNumbers.length) return;
        const partNumber = partNumbers[cursor];
        cursor += 1;
        await uploadPartWithRetry(
          file,
          currentUploadId,
          partNumber,
          partURLs,
          completedParts,
          completedEtags,
          totalChunks,
        );
      }
    };
    await Promise.all(Array.from({ length: Math.min(concurrency, partNumbers.length) }, worker));
  };

  const completeWithRepair = async (file, currentUploadId, completedParts, completedEtags, totalChunks) => {
    for (let round = 0; round <= MAX_COMPLETE_REPAIR_ROUNDS; round += 1) {
      try {
        if (canceled.value) throw new Error("上传已取消");
        status.value = UploadStatus.COMPLETING;
        // 标准 complete：回传每片的 (partNumber, etag)
        const parts = [...completedEtags.entries()]
          .map(([partNumber, etag]) => ({ partNumber, etag }))
          .sort((a, b) => a.partNumber - b.partNumber);
        const result = await completeFileUpload(currentUploadId, parts);
        if (result?.status !== UploadStatus.COMPLETED || !result.fileId) {
          throw new Error("合并成功但未返回文件标识。");
        }
        return result;
      } catch (completeError) {
        const repairParts = incompletePartNumbers(completeError);
        if (!repairParts.length || round === MAX_COMPLETE_REPAIR_ROUNDS) throw completeError;
        status.value = UploadStatus.UPLOADING;
        await uploadParts(
          file,
          currentUploadId,
          repairParts,
          completedParts,
          completedEtags,
          totalChunks,
        );
      }
    }
    throw new Error("合并文件失败");
  };

  const uploadMultipart = async (file, initialized) => {
    const totalChunks = initialized.totalChunks;
    const completedParts = new Set(initialized.uploadedParts || []);
    const completedEtags = new Map();
    updateMultipartProgress(completedParts, totalChunks);

    const queue = Array.from({ length: totalChunks }, (_, index) => index + 1).filter(
      (partNumber) => !completedParts.has(partNumber),
    );
    status.value = UploadStatus.UPLOADING;
    await uploadParts(file, initialized.uploadId, queue, completedParts, completedEtags, totalChunks);
    if (canceled.value) throw new Error("上传已取消");
    return completeWithRepair(file, initialized.uploadId, completedParts, completedEtags, totalChunks);
  };

  const upload = async (file) => {
    reset();
    try {
      if (!file || file.size <= 0) throw new Error("文件不能为空");
      assertFileCanBeHashed(file, maxFileSize, maxHashFileSize);
      status.value = UploadStatus.HASHING;
      const fileHash = await createActualSHA256(file);
      if (canceled.value) throw new Error("上传已取消");

      // 标准流程：先向服务端问分片规格（partSize/totalChunks），本地不再自算
      status.value = UploadStatus.INITIALIZING;
      const spec = await getPartSize(file.size);
      partSize = spec.partSize;
      const userId = currentUserId();
      const objectName = `${userId}/${createObjectName(file.name)}`;
      const initialized = await initFileUpload({
        name: objectName,
        fileName: file.name,
        contentType: file.type || "application/octet-stream",
        size: file.size,
        fileHash,
        partSize,
      });
      if (canceled.value) throw new Error("上传已取消");

      if (initialized.status === UploadStatus.COMPLETED) {
        if (!initialized.fileId) throw new Error("秒传响应缺少文件标识。");
        status.value = UploadStatus.COMPLETED;
        progress.value = 100;
        return { fileId: initialized.fileId, status: initialized.status };
      }

      // 新版统一走分片直传，没有直传分支
      const result = await uploadMultipart(file, initialized);
      if (canceled.value) throw new Error("上传已取消");
      status.value = UploadStatus.COMPLETED;
      progress.value = 100;
      return result;
    } catch (uploadError) {
      if (!canceled.value) status.value = UploadStatus.FAILED;
      throw uploadError;
    }
  };

  const pause = () => {
    if (status.value === UploadStatus.UPLOADING) {
      paused.value = true;
      status.value = UploadStatus.PAUSED;
    }
  };

  const resume = () => {
    if (status.value === UploadStatus.PAUSED) {
      if (releasePause) {
        releasePause();
      } else {
        paused.value = false;
        resumePromise = null;
      }
      status.value = UploadStatus.UPLOADING;
    }
  };

  const cancel = () => {
    canceled.value = true;
    paused.value = false;
    releasePause?.();
    releasePause = null;
    resumePromise = null;
    activeRequests.forEach((controller) => controller.abort());
    activeRequests.clear();
    status.value = UploadStatus.CANCELED;
  };

  return {
    status,
    progress,
    upload,
    pause,
    resume,
    cancel,
  };
}

// 与 authService.readStoredUser 保持同一存储源，用于构造 name 的 userID 前缀
function currentUserId() {
  if (typeof window === "undefined") return "unknown";
  try {
    const raw =
      window.localStorage?.getItem("im_user") || window.sessionStorage?.getItem("im_user") || "{}";
    return JSON.parse(raw)?.userId || "unknown";
  } catch {
    return "unknown";
  }
}

// 对象名用 uuid + 原扩展名，避免同名文件互相覆盖
function createObjectName(fileName = "") {
  const ext = fileName.includes(".") ? fileName.slice(fileName.lastIndexOf(".")) : "";
  const uuid =
    globalThis.crypto?.randomUUID?.() ||
    `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `${uuid}${ext}`;
}

async function createActualSHA256(file) {
  const result = await crypto.subtle.digest("SHA-256", await file.arrayBuffer());
  return Array.from(new Uint8Array(result), (value) => value.toString(16).padStart(2, "0")).join("");
}

function assertFileCanBeHashed(file, maxFileSize, maxHashFileSize) {
  if (file.size > maxFileSize) {
    throw new Error("文件大小超过允许上传的上限。");
  }

  if (file.size > maxHashFileSize) {
    throw new Error("当前设备不适合在浏览器中计算该文件的完整 Hash。");
  }

  const deviceMemoryGB = Number(globalThis.navigator?.deviceMemory);
  if (!Number.isFinite(deviceMemoryGB) || deviceMemoryGB <= 0) return;

  const estimatedHashMemory = file.size * HASH_MEMORY_MULTIPLIER;
  const deviceHashMemoryLimit = deviceMemoryGB * 1024 * 1024 * 1024 * DEVICE_HASH_MEMORY_RATIO;
  if (estimatedHashMemory > deviceHashMemoryLimit) {
    throw new Error("当前设备可用内存不足，无法安全计算该文件的完整 Hash。");
  }
}

const sleep = (duration) => new Promise((resolve) => window.setTimeout(resolve, duration));

function incompletePartNumbers(error) {
  if (error?.status !== 409 || error.data?.status !== UploadStatus.UPLOADING) return [];
  const data = error.data || {};
  const numbers = [...(data.missingParts || []), ...(data.invalidParts || [])]
    .map((partNumber) => Number(partNumber))
    .filter((partNumber) => Number.isInteger(partNumber) && partNumber > 0);
  return [...new Set(numbers)].sort((a, b) => a - b);
}
