export const createAttachmentResolver = (getAccessURLs) => {
  const cache = new Map();

  /** 批量解析消息附件的临时访问地址，并只回填内存中的展示字段。 */
  const resolve = async (messages) => {
    const candidates = (messages || []).filter((message) => message.attachmentId);
    if (!candidates.length) return messages;

    const attachmentIds = [...new Set(candidates.map((message) => message.attachmentId))];
    const pendingIds = [];
    const now = Math.floor(Date.now() / 1000);

    for (const attachmentId of attachmentIds) {
      const cached = cache.get(attachmentId);
      if (cached && Number(cached.expiresAt) > now + 30 && cached.mediaUrl) continue;

      cache.delete(attachmentId);
      pendingIds.push(attachmentId);
    }

    if (pendingIds.length) {
      try {
        const result = await getAccessURLs(pendingIds);
        for (const card of result?.attachments || []) {
          cache.set(card.attachmentId, card);
        }
      } catch {
        // 访问地址失效或无权限时，保留消息元数据，不阻断消息列表。
      }
    }

    for (const message of candidates) {
      const card = cache.get(message.attachmentId);
      if (!card?.mediaUrl) continue;

      message.mediaUrl = card.mediaUrl;
      message.thumbUrl = card.thumbUrl || card.mediaUrl;
      if (card.fileName) message.fileName = card.fileName;
      if (card.contentType) message.mimeType = card.contentType;
      if (Number.isFinite(Number(card.size))) message.fileSize = Number(card.size);
    }

    return messages;
  };

  return {
    resolve,
    clear: () => cache.clear(),
  };
};
