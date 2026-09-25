const http = require('http');
const { Server } = require('socket.io');

const port = parseInt(process.env.PORT || '4000', 10);
const adminSecretPassword = (process.env.ADMIN_PASSWORD || '').trim();

// Active admin tokens set
const activeAdminTokens = new Set();
if (process.env.ADMIN_TOKEN) {
  activeAdminTokens.add(process.env.ADMIN_TOKEN.trim());
}

// In-memory stream state
const streamState = {
  id: 'main-stream',
  title: 'Football Live — English Commentary & Match Analysis',
  description: 'Broadcasting live high-definition screen and commentary. Join the real-time chat and enjoy the stream!',
  category: 'Sports & Live Action',
  status: 'offline', // 'offline' | 'live'
  streamerSocketId: null,
  streamerName: 'Official Streamer',
  started_at: null,
  ended_at: null,
  current_viewers: 0,
  peak_viewers: 0,
  total_unique_viewers: 0,
};

const viewersPresence = new Map();
viewersPresence.set(streamState.id, new Map());
const uniqueViewersSet = new Set();
const chatMessages = [];
const bannedUsers = new Map();
const rateLimitMap = new Map();

function sanitizeText(str) {
  if (typeof str !== 'string') return '';
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;')
    .trim();
}

function updateViewerStats(io, streamId) {
  const streamViewers = viewersPresence.get(streamId) || new Map();
  const currentCount = streamViewers.size;
  streamState.current_viewers = currentCount;
  if (currentCount > streamState.peak_viewers) {
    streamState.peak_viewers = currentCount;
  }
  streamState.total_unique_viewers = uniqueViewersSet.size;

  io.to(streamId).emit('stream:viewer-count', {
    current: streamState.current_viewers,
    peak: streamState.peak_viewers,
    totalUnique: streamState.total_unique_viewers,
  });
}

// HTTP Server
const server = http.createServer((req, res) => {
  // CORS Headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-admin-token');

  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    return res.end();
  }

  // Health check endpoint
  if (req.url === '/health' || req.url === '/') {
    res.setHeader('Content-Type', 'application/json');
    return res.end(
      JSON.stringify({
        status: 'healthy',
        service: 'StreamPulse Signaling Server',
        timestamp: new Date().toISOString(),
        viewers: streamState.current_viewers,
        streamStatus: streamState.status,
      })
    );
  }

  // Stream state endpoint
  if (req.url === '/api/stream/state') {
    res.setHeader('Content-Type', 'application/json');
    return res.end(
      JSON.stringify({
        ...streamState,
        duration: streamState.started_at
          ? Math.floor((Date.now() - new Date(streamState.started_at).getTime()) / 1000)
          : 0,
        messagesCount: chatMessages.length,
      })
    );
  }

  res.statusCode = 404;
  res.end('Not Found');
});

// Socket.IO Server
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
  transports: ['websocket', 'polling'],
});

// Stale viewer cleanup loop (runs every 6 seconds)
setInterval(() => {
  const now = Date.now();
  const streamViewers = viewersPresence.get(streamState.id);
  if (!streamViewers) return;

  let changed = false;
  for (const [viewerId, viewer] of streamViewers.entries()) {
    if (now - viewer.lastHeartbeat > 25000) {
      streamViewers.delete(viewerId);
      changed = true;
    }
  }

  if (changed) {
    updateViewerStats(io, streamState.id);
  }
}, 6000);

io.on('connection', (socket) => {
  socket.isAdmin = false;

  // 1. ADMIN AUTHENTICATION
  socket.on('admin:auth', (data) => {
    const token = data?.token;
    const isPasscodeMatch = adminSecretPassword && (token === adminSecretPassword || data?.password === adminSecretPassword);
    const isTokenMatch = token && activeAdminTokens.has(token);

    if (isPasscodeMatch || isTokenMatch || (!adminSecretPassword && token)) {
      socket.isAdmin = true;
      socket.join('admin-room');
      socket.emit('admin:auth-success');
    } else {
      socket.isAdmin = false;
      socket.emit('admin:auth-failed', { message: 'Invalid admin credentials.' });
    }
  });

  // 2. BROADCAST CONTROL (Protected)
  socket.on('broadcaster:start', (data) => {
    if (!socket.isAdmin) {
      return socket.emit('admin:error', { message: 'Unauthorized. Admin authorization required.' });
    }

    streamState.status = 'live';
    streamState.streamerSocketId = socket.id;
    streamState.title = sanitizeText(data?.title || streamState.title) || streamState.title;
    streamState.description = sanitizeText(data?.description || streamState.description) || streamState.description;
    streamState.category = sanitizeText(data?.category || streamState.category) || streamState.category;
    streamState.started_at = new Date().toISOString();
    streamState.ended_at = null;

    socket.join(streamState.id);

    io.to(streamState.id).emit('stream:status-changed', {
      status: 'live',
      title: streamState.title,
      description: streamState.description,
      category: streamState.category,
      started_at: streamState.started_at,
    });

    socket.to(streamState.id).emit('broadcaster:ready', {
      streamerSocketId: socket.id,
    });
  });

  socket.on('broadcaster:stop', () => {
    if (!socket.isAdmin) {
      return socket.emit('admin:error', { message: 'Unauthorized. Admin authorization required.' });
    }

    streamState.status = 'offline';
    streamState.streamerSocketId = null;
    streamState.ended_at = new Date().toISOString();

    io.to(streamState.id).emit('stream:status-changed', {
      status: 'offline',
      ended_at: streamState.ended_at,
    });

    io.to(streamState.id).emit('stream:stopped');
  });

  socket.on('broadcaster:update-info', (data) => {
    if (!socket.isAdmin) {
      return socket.emit('admin:error', { message: 'Unauthorized. Admin authorization required.' });
    }

    if (data?.title) streamState.title = sanitizeText(data.title);
    if (data?.description) streamState.description = sanitizeText(data.description);
    if (data?.category) streamState.category = sanitizeText(data.category);

    io.to(streamState.id).emit('stream:info-updated', {
      title: streamState.title,
      description: streamState.description,
      category: streamState.category,
    });
  });

  // 3. WEBRTC SIGNALING RELAY
  socket.on('webrtc:viewer-ready', (data) => {
    if (streamState.streamerSocketId) {
      io.to(streamState.streamerSocketId).emit('webrtc:new-viewer', {
        viewerSocketId: socket.id,
        viewerId: data?.viewerId,
      });
    }
  });

  socket.on('webrtc:offer', (data) => {
    if (data?.targetSocketId && data?.offer) {
      io.to(data.targetSocketId).emit('webrtc:offer', {
        offer: data.offer,
        fromSocketId: socket.id,
      });
    }
  });

  socket.on('webrtc:answer', (data) => {
    if (data?.targetSocketId && data?.answer) {
      io.to(data.targetSocketId).emit('webrtc:answer', {
        answer: data.answer,
        fromSocketId: socket.id,
      });
    }
  });

  socket.on('webrtc:ice-candidate', (data) => {
    if (data?.targetSocketId && data?.candidate) {
      io.to(data.targetSocketId).emit('webrtc:ice-candidate', {
        candidate: data.candidate,
        fromSocketId: socket.id,
      });
    }
  });

  // 4. VIEWER PRESENCE
  socket.on('viewer:join', (data) => {
    const viewerId = data?.viewerId || socket.id;
    const username = sanitizeText(data?.username || 'Viewer');
    const streamId = data?.streamId || streamState.id;

    socket.join(streamId);
    socket.data = { viewerId, username, streamId };

    uniqueViewersSet.add(viewerId);

    const streamViewers = viewersPresence.get(streamId) || new Map();
    streamViewers.set(viewerId, {
      socketId: socket.id,
      username,
      joinedAt: Date.now(),
      lastHeartbeat: Date.now(),
    });
    viewersPresence.set(streamId, streamViewers);

    socket.emit('stream:init', {
      stream: streamState,
      messages: chatMessages.slice(-50),
    });

    updateViewerStats(io, streamId);

    if (streamState.status === 'live' && streamState.streamerSocketId) {
      socket.emit('broadcaster:ready', {
        streamerSocketId: streamState.streamerSocketId,
      });
    }
  });

  socket.on('viewer:heartbeat', (data) => {
    const viewerId = data?.viewerId || socket.data?.viewerId;
    const streamId = data?.streamId || streamState.id;
    const streamViewers = viewersPresence.get(streamId);
    if (streamViewers && viewerId && streamViewers.has(viewerId)) {
      const v = streamViewers.get(viewerId);
      v.lastHeartbeat = Date.now();
    }
  });

  socket.on('viewer:leave', (data) => {
    const viewerId = data?.viewerId || socket.data?.viewerId;
    const streamId = data?.streamId || streamState.id;
    const streamViewers = viewersPresence.get(streamId);
    if (streamViewers && viewerId) {
      streamViewers.delete(viewerId);
      updateViewerStats(io, streamId);
    }
  });

  // 5. CHAT & MODERATION
  socket.on('chat:send-message', (data) => {
    const userId = data?.user_id || socket.id;
    const username = sanitizeText(data?.username || 'Viewer');
    const role = socket.isAdmin ? 'admin' : 'viewer';
    const rawMessage = data?.message;

    const banInfo = bannedUsers.get(userId);
    if (banInfo) {
      if (!banInfo.timeoutUntil || Date.now() < banInfo.timeoutUntil) {
        return socket.emit('chat:error', {
          message: banInfo.timeoutUntil
            ? `You are timed out until ${new Date(banInfo.timeoutUntil).toLocaleTimeString()}`
            : 'You have been banned from sending chat messages.',
        });
      } else {
        bannedUsers.delete(userId);
      }
    }

    const lastSent = rateLimitMap.get(socket.id) || 0;
    if (Date.now() - lastSent < 400 && !socket.isAdmin) {
      return socket.emit('chat:error', { message: 'You are typing too fast. Please slow down.' });
    }
    rateLimitMap.set(socket.id, Date.now());

    if (!rawMessage || typeof rawMessage !== 'string') return;
    const cleanMessage = sanitizeText(rawMessage);
    if (cleanMessage.length === 0 || cleanMessage.length > 300) {
      return socket.emit('chat:error', { message: 'Message must be between 1 and 300 characters.' });
    }

    const newMsg = {
      id: 'msg_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7),
      stream_id: streamState.id,
      user_id: userId,
      username: username,
      role: role,
      message: cleanMessage,
      created_at: new Date().toISOString(),
      is_deleted: false,
    };

    chatMessages.push(newMsg);
    if (chatMessages.length > 200) chatMessages.shift();

    io.to(streamState.id).emit('chat:new-message', newMsg);
  });

  socket.on('chat:delete-message', (data) => {
    if (!socket.isAdmin) {
      return socket.emit('admin:error', { message: 'Unauthorized. Admin authorization required.' });
    }
    const messageId = data?.messageId;
    if (!messageId) return;

    const target = chatMessages.find((m) => m.id === messageId);
    if (target) {
      target.is_deleted = true;
      target.message = 'This message was removed by a moderator.';
    }

    io.to(streamState.id).emit('chat:message-deleted', { messageId });
  });

  socket.on('chat:timeout-user', (data) => {
    if (!socket.isAdmin) {
      return socket.emit('admin:error', { message: 'Unauthorized. Admin authorization required.' });
    }
    const { userId, username, durationSeconds = 60, reason } = data || {};
    if (!userId) return;

    const timeoutUntil = Date.now() + durationSeconds * 1000;
    bannedUsers.set(userId, { reason, timeoutUntil });

    io.to(streamState.id).emit('chat:system-notice', {
      notice: `User @${username || userId} has been timed out for ${durationSeconds}s.`,
    });
  });

  socket.on('chat:ban-user', (data) => {
    if (!socket.isAdmin) {
      return socket.emit('admin:error', { message: 'Unauthorized. Admin authorization required.' });
    }
    const { userId, username, reason } = data || {};
    if (!userId) return;

    bannedUsers.set(userId, { reason: reason || 'Banned by admin', timeoutUntil: null });

    io.to(streamState.id).emit('chat:system-notice', {
      notice: `User @${username || userId} has been banned from chat.`,
    });
  });

  socket.on('chat:clear-chat', () => {
    if (!socket.isAdmin) {
      return socket.emit('admin:error', { message: 'Unauthorized. Admin authorization required.' });
    }
    chatMessages.length = 0;
    io.to(streamState.id).emit('chat:cleared');
  });

  socket.on('disconnect', () => {
    rateLimitMap.delete(socket.id);

    if (socket.id === streamState.streamerSocketId) {
      streamState.status = 'offline';
      streamState.streamerSocketId = null;
      streamState.ended_at = new Date().toISOString();
      io.to(streamState.id).emit('stream:status-changed', { status: 'offline' });
      io.to(streamState.id).emit('stream:stopped');
    }

    if (socket.data?.viewerId) {
      const streamViewers = viewersPresence.get(socket.data.streamId || streamState.id);
      if (streamViewers) {
        streamViewers.delete(socket.data.viewerId);
        updateViewerStats(io, socket.data.streamId || streamState.id);
      }
    }
  });
});

server.listen(port, () => {
  console.log(`> StreamPulse Signaling & Realtime Server active on port ${port}`);
});
