import fs from 'node:fs';
import path from 'node:path';

const apiBase = (process.env.API_BASE_URL || 'http://127.0.0.1:8081/api/v1').replace(/\/$/, '');
const fixturePath = process.env.FIXTURE_FILE || '/tmp/linkchat-room-load-fixture.json';
const refreshTokens = process.env.REFRESH_TOKENS === '1';
const existingFixture = refreshTokens ? JSON.parse(fs.readFileSync(fixturePath, 'utf8')) : null;
const userCount = Number(process.env.USER_COUNT || 500);
const password = process.env.TEST_PASSWORD || existingFixture?.password;
const concurrency = Number(process.env.SETUP_CONCURRENCY || 6);
const origin = process.env.WEB_ORIGIN || 'http://127.0.0.1:5173';

if (!password) throw new Error('请通过 TEST_PASSWORD 设置压测账号密码');
if (!Number.isInteger(userCount) || userCount < 1) {
  throw new Error('USER_COUNT 必须是正整数');
}
if (fs.existsSync(fixturePath) && !refreshTokens && process.env.OVERWRITE_FIXTURE !== '1') {
  throw new Error(`压测凭据文件已存在：${fixturePath}；设置 OVERWRITE_FIXTURE=1 才会覆盖`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function request(route, { token, method = 'GET', body } = {}) {
  let lastError;
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      const response = await fetch(`${apiBase}${route}`, {
        method,
        headers: {
          Origin: origin,
          ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(20000),
      });
      const payload = await response.json().catch(() => ({}));
      if ((response.status === 429 || response.status >= 500) && attempt < 4) {
        await sleep(250 * (attempt + 1));
        continue;
      }
      return { response, payload };
    } catch (error) {
      lastError = error;
      if (attempt < 4) await sleep(250 * (attempt + 1));
    }
  }
  throw lastError || new Error(`请求失败：${route}`);
}

function accessToken(response) {
  const cookies = response.headers.getSetCookie?.() || [];
  const cookie = cookies.find((item) => item.startsWith('access_token='));
  return cookie?.slice('access_token='.length).split(';', 1)[0] || '';
}

async function login(phone) {
  const { response, payload } = await request('/users/login', {
    method: 'POST',
    body: { account: phone, password },
  });
  const token = accessToken(response);
  if (response.status !== 200 || payload.code !== 200 || !token || !payload.data?.userId) {
    throw new Error(`账号登录失败：phone=${phone} status=${response.status} code=${payload.code}`);
  }
  return { phone, userId: payload.data.userId, token };
}

let phoneCursor = Date.now() % 100000000;
async function createAccount() {
  for (let attempt = 0; attempt < 10000; attempt++) {
    const suffix = String(phoneCursor++ % 100000000).padStart(8, '0');
    const phone = `199${suffix}`;
    const { response, payload } = await request('/users/register', {
      method: 'POST',
      body: {
        phone,
        password,
        reconfirmPassword: password,
        loginType: 1,
      },
    });

    if (response.status === 409) continue;
    if (response.status !== 200 || payload.code !== 200) {
      throw new Error(`账号注册失败：phone=${phone} status=${response.status} code=${payload.code}`);
    }
    return login(phone);
  }
  throw new Error('连续遇到手机号冲突，停止创建压测账号');
}

async function saveFixture(fixture) {
  fs.mkdirSync(path.dirname(fixturePath), { recursive: true, mode: 0o700 });
  fs.writeFileSync(fixturePath, `${JSON.stringify(fixture, null, 2)}\n`, { mode: 0o600 });
  fs.chmodSync(fixturePath, 0o600);
}

async function parallel(items, limit, fn) {
  let cursor = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (cursor < items.length) {
      const index = cursor++;
      await fn(items[index], index);
    }
  }));
}

async function createRoom(owner) {
  const suffix = new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14);
  const { response, payload } = await request('/rooms', {
    method: 'POST',
    token: owner.token,
    body: { roomName: `k6-room-load-${suffix}`, description: '自动生成的本地压测房间' },
  });
  const room = payload.data;
  if (response.status !== 200 || payload.code !== 200 || !room?.roomId || !room?.inviteCode) {
    throw new Error(`创建压测房间失败：status=${response.status} code=${payload.code}`);
  }
  return room;
}

async function getInviteCode(owner, roomId) {
  const { response, payload } = await request(`/rooms/${roomId}/invite-code`, { token: owner.token });
  if (response.status !== 200 || payload.code !== 200 || typeof payload.data !== 'string') {
    throw new Error(`获取压测房间邀请码失败：status=${response.status} code=${payload.code}`);
  }
  return payload.data;
}

async function joinRoom(user, inviteCode) {
  const { response, payload } = await request('/rooms/join', {
    method: 'POST',
    token: user.token,
    body: { inviteCode },
  });
  if (response.status !== 200 || payload.code !== 200 || !payload.data?.conversationId) {
    throw new Error(`加入压测房间失败：userId=${user.userId} status=${response.status} code=${payload.code}`);
  }
  return payload.data.conversationId;
}

async function getConversationId(owner, roomId) {
  const { response, payload } = await request('/conversations', { token: owner.token });
  const conversations = payload.data;
  const roomConversation = Array.isArray(conversations)
    ? conversations.find((item) => item.targetId === roomId || item.room?.roomId === roomId)
    : null;
  if (response.status !== 200 || payload.code !== 200 || !roomConversation?.conversationId) {
    throw new Error(`查询压测房间会话失败：status=${response.status} code=${payload.code}`);
  }
  return roomConversation;
}

if (refreshTokens) {
  if (existingFixture.users.length < userCount) {
    throw new Error(`账号文件只有 ${existingFixture.users.length} 个用户，无法刷新 ${userCount} 个`);
  }
  await parallel(existingFixture.users.slice(0, userCount), concurrency, async (user, index) => {
    existingFixture.users[index] = await login(user.phone);
  });
  existingFixture.tokensRefreshedAt = new Date().toISOString();
  await saveFixture(existingFixture);
  console.log(JSON.stringify({ fixtureFile: fixturePath, refreshedUsers: userCount }));
} else {
  const owner = await createAccount();
  const room = await createRoom(owner);
  const fixture = {
    roomId: room.roomId,
    conversationId: '',
    userCount,
    password,
    createdAt: new Date().toISOString(),
    users: [owner],
  };
  await saveFixture(fixture);

  const remaining = Array.from({ length: userCount - 1 }, (_, index) => index + 1);
  for (let start = 0; start < remaining.length; start += 40) {
    const batch = remaining.slice(start, start + 40);
    const inviteCode = await getInviteCode(owner, room.roomId);
    const created = [];
    await parallel(batch, concurrency, async () => {
      const user = await createAccount();
      await joinRoom(user, inviteCode);
      created.push(user);
    });
    fixture.users.push(...created);
    await saveFixture(fixture);
    console.log(`已加入压测房间：${fixture.users.length}/${userCount}`);
  }

  const conversation = await getConversationId(owner, room.roomId);
  fixture.conversationId = conversation.conversationId;
  fixture.initialSeq = Number(conversation.latestSeq) || 0;
  await saveFixture(fixture);
  console.log(JSON.stringify({
    fixtureFile: fixturePath,
    roomId: fixture.roomId,
    conversationId: fixture.conversationId,
    users: fixture.users.length,
    accessTokenExpiresInMinutes: 150,
  }));
}
