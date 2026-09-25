const http = require('http');
const { Server } = require('socket.io');

const port = parseInt(process.env.PORT || '4000', 10);

// Allowed Origins from environment variable or defaults
const rawAllowedOrigins = process.env.ALLOWED_ORIGINS || '';
const customOrigins = rawAllowedOrigins
  ? rawAllowedOrigins.split(',').map((o) => o.trim()).filter(Boolean)
  : [];

const defaultOrigins = [
  'http://localhost:3000',
  'http://localhost:3001',
  'http://127.0.0.1:3000',
  'http://127.0.0.1:3001',
  'https://streampulse-admin.vercel.app',
  'https://streampulse-user.vercel.app',
];

const allowedOriginsSet = new Set([...defaultOrigins, ...customOrigins]);

function isOriginAllowed(origin) {
  if (!origin) return true; // allow curl, native clients, server-to-server
  if (allowedOriginsSet.has(origin)) return true;
  if (origin.endsWith('.vercel.app')) return true;
  if (rawAllowedOrigins === '*') return true;
  return false;
}

// In-memory stream state
const streamState = {
  id: 'main-stream',
  title: 'Football Live — English Commentary & Match Analysis',
  description: 'Broadcasting live high-definition screen and commentary. Join the real-time chat and enjoy the stream!',
  category: 'Sports & Live Action',
  status: 'offline',
  streamerSocketId: null,
  streamerName: 'Host Broadcaster',
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

function broadcastViewerStats(io, adminNs, streamId) {
  const streamViewers = viewersPresence.get(streamId) || new Map();
  const currentCount = streamViewers.size;
  streamState.current_viewers = currentCount;
  if (currentCount > streamState.peak_viewers) {
    streamState.peak_viewers = currentCount;
  }
  streamState.total_unique_viewers = uniqueViewersSet.size;

  const payload = {
    current: streamState.current_viewers,
    peak: streamState.peak_viewers,
    totalUnique: streamState.total_unique_viewers,
  };

  io.to(streamId).emit('stream:viewer-count', payload);
  adminNs.emit('stream:viewer-count', payload);
}

// HTTP Server
const server = http.createServer((req, res) => {
  const origin = req.headers.origin;

  if (isOriginAllowed(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin || '*');
    res.setHeader('Access-Control-Allow-Credentials', 'true');
  } else {
    res.setHeader('Access-Control-Allow-Origin', '*');
  }

  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    return res.end();
  }

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
    origin: (origin, callback) => {
      if (isOriginAllowed(origin)) {
        callback(null, true);
      } else {
        callback(null, true); // Allow viewers from web origins
      }
    },
    methods: ['GET', 'POST'],
    credentials: true,
  },
  transports: ['websocket', 'polling'],
});

const adminNs = io.of('/admin');

// Periodic stale presence cleanup (every 6s)
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
    broadcastViewerStats(io, adminNs, streamState.id);
  }
}, 6000);

// ==========================================
// 1. ADMIN / BROADCASTER NAMESPACE (/admin)
// ==========================================
adminNs.on('connection', (socket) => {
  console.log(`[Admin Socket] Broadcaster connected: ${socket.id}`);

  // Send current state and chat history on connect
  socket.emit('stream:init', {
    stream: streamState,
    messages: chatMessages.slice(-50),
  });

  socket.emit('stream:viewer-count', {
    current: streamState.current_viewers,
    peak: streamState.peak_viewers,
    totalUnique: streamState.total_unique_viewers,
  });

  // Start Broadcast
  socket.on('broadcaster:start', (data) => {
    streamState.status = 'live';
    streamState.streamerSocketId = socket.id;
    streamState.title = sanitizeText(data?.title || streamState.title) || streamState.title;
    streamState.description = sanitizeText(data?.description || streamState.description) || streamState.description;
    streamState.category = sanitizeText(data?.category || streamState.category) || streamState.category;
    streamState.started_at = new Date().toISOString();
    streamState.ended_at = null;

    const statusPayload = {
      status: 'live',
      title: streamState.title,
      description: streamState.description,
      category: streamState.category,
      started_at: streamState.started_at,
    };

    io.to(streamState.id).emit('stream:status-changed', statusPayload);
    adminNs.emit('stream:status-changed', statusPayload);

    io.to(streamState.id).emit('broadcaster:ready', {
      streamerSocketId: socket.id,
    });

    console.log(`[Stream] Live broadcast started: "${streamState.title}" by Admin (${socket.id})`);
  });

  // Stop Broadcast
  socket.on('broadcaster:stop', () => {
    streamState.status = 'offline';
    streamState.streamerSocketId = null;
    streamState.ended_at = new Date().toISOString();

    const statusPayload = {
      status: 'offline',
      ended_at: streamState.ended_at,
    };

    io.to(streamState.id).emit('stream:status-changed', statusPayload);
    adminNs.emit('stream:status-changed', statusPayload);

    io.to(streamState.id).emit('stream:stopped');
    adminNs.emit('stream:stopped');

    console.log(`[Stream] Broadcast stopped by Admin (${socket.id})`);
  });

  // Update Stream Metadata
  socket.on('broadcaster:update-info', (data) => {
    if (data?.title) streamState.title = sanitizeText(data.title);
    if (data?.description) streamState.description = sanitizeText(data.description);
    if (data?.category) streamState.category = sanitizeText(data.category);

    const updatePayload = {
      title: streamState.title,
      description: streamState.description,
      category: streamState.category,
    };

    io.to(streamState.id).emit('stream:info-updated', updatePayload);
    adminNs.emit('stream:info-updated', updatePayload);
  });

  // WebRTC Offer from Broadcaster to Viewer
  socket.on('webrtc:offer', (data) => {
    if (data?.targetSocketId && data?.offer) {
      io.to(data.targetSocketId).emit('webrtc:offer', {
        offer: data.offer,
        fromSocketId: socket.id,
      });
    }
  });

  // WebRTC ICE Candidate from Broadcaster to Viewer
  socket.on('webrtc:ice-candidate', (data) => {
    if (data?.targetSocketId && data?.candidate) {
      io.to(data.targetSocketId).emit('webrtc:ice-candidate', {
        candidate: data.candidate,
        fromSocketId: socket.id,
      });
    }
  });

  // Admin / Broadcaster Chat Message
  socket.on('chat:send-message', (data) => {
    const rawMessage = data?.message;
    if (!rawMessage || typeof rawMessage !== 'string') return;
    const cleanMessage = sanitizeText(rawMessage);
    if (cleanMessage.length === 0 || cleanMessage.length > 300) return;

    const newMsg = {
      id: 'msg_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7),
      stream_id: streamState.id,
      user_id: 'admin_broadcaster',
      username: sanitizeText(data?.username || 'Host Broadcaster'),
      role: 'admin',
      message: cleanMessage,
      created_at: new Date().toISOString(),
      is_deleted: false,
    };

    chatMessages.push(newMsg);
    if (chatMessages.length > 200) chatMessages.shift();

    io.to(streamState.id).emit('chat:new-message', newMsg);
    adminNs.emit('chat:new-message', newMsg);
  });

  // Moderation: Delete Message
  socket.on('chat:delete-message', (data) => {
    const messageId = data?.messageId;
    if (!messageId) return;

    const target = chatMessages.find((m) => m.id === messageId);
    if (target) {
      target.is_deleted = true;
      target.message = 'This message was removed by a moderator.';
    }

    io.to(streamState.id).emit('chat:message-deleted', { messageId });
    adminNs.emit('chat:message-deleted', { messageId });
  });

  // Moderation: Timeout User
  socket.on('chat:timeout-user', (data) => {
    const { userId, username, durationSeconds = 60, reason } = data || {};
    if (!userId) return;

    const timeoutUntil = Date.now() + durationSeconds * 1000;
    bannedUsers.set(userId, { reason, timeoutUntil });

    const noticePayload = {
      notice: `User @${username || userId} has been timed out for ${durationSeconds}s.`,
    };

    io.to(streamState.id).emit('chat:system-notice', noticePayload);
    adminNs.emit('chat:system-notice', noticePayload);
  });

  // Moderation: Ban User
  socket.on('chat:ban-user', (data) => {
    const { userId, username, reason } = data || {};
    if (!userId) return;

    bannedUsers.set(userId, { reason: reason || 'Banned by admin', timeoutUntil: null });

    const noticePayload = {
      notice: `User @${username || userId} has been banned from chat.`,
    };

    io.to(streamState.id).emit('chat:system-notice', noticePayload);
    adminNs.emit('chat:system-notice', noticePayload);
  });

  // Moderation: Clear Chat
  socket.on('chat:clear-chat', () => {
    chatMessages.length = 0;
    io.to(streamState.id).emit('chat:cleared');
    adminNs.emit('chat:cleared');
  });

  socket.on('disconnect', () => {
    console.log(`[Admin Socket] Broadcaster disconnected: ${socket.id}`);
    if (socket.id === streamState.streamerSocketId) {
      streamState.status = 'offline';
      streamState.streamerSocketId = null;
      streamState.ended_at = new Date().toISOString();

      io.to(streamState.id).emit('stream:status-changed', { status: 'offline' });
      io.to(streamState.id).emit('stream:stopped');
      adminNs.emit('stream:status-changed', { status: 'offline' });
      adminNs.emit('stream:stopped');
    }
  });
});

// ==========================================
// 2. PUBLIC VIEWER NAMESPACE (/)
// ==========================================
io.on('connection', (socket) => {
  // Public Viewer Join
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

    broadcastViewerStats(io, adminNs, streamId);

    if (streamState.status === 'live' && streamState.streamerSocketId) {
      socket.emit('broadcaster:ready', {
        streamerSocketId: streamState.streamerSocketId,
      });
    }
  });

  // Viewer Heartbeat
  socket.on('viewer:heartbeat', (data) => {
    const viewerId = data?.viewerId || socket.data?.viewerId;
    const streamId = data?.streamId || streamState.id;
    const streamViewers = viewersPresence.get(streamId);
    if (streamViewers && viewerId && streamViewers.has(viewerId)) {
      const v = streamViewers.get(viewerId);
      v.lastHeartbeat = Date.now();
    }
  });

  // Viewer Leave
  socket.on('viewer:leave', (data) => {
    const viewerId = data?.viewerId || socket.data?.viewerId;
    const streamId = data?.streamId || streamState.id;
    const streamViewers = viewersPresence.get(streamId);
    if (streamViewers && viewerId) {
      streamViewers.delete(viewerId);
      broadcastViewerStats(io, adminNs, streamId);
    }
  });

  // WebRTC Signaling: Viewer Ready
  socket.on('webrtc:viewer-ready', (data) => {
    adminNs.emit('webrtc:new-viewer', {
      viewerSocketId: socket.id,
      viewerId: data?.viewerId || socket.data?.viewerId,
    });
  });

  // WebRTC Signaling: Viewer Answer
  socket.on('webrtc:answer', (data) => {
    if (data?.answer) {
      adminNs.emit('webrtc:answer', {
        answer: data.answer,
        fromSocketId: socket.id,
      });
    }
  });

  // WebRTC Signaling: Viewer ICE Candidate
  socket.on('webrtc:ice-candidate', (data) => {
    if (data?.candidate) {
      adminNs.emit('webrtc:ice-candidate', {
        candidate: data.candidate,
        fromSocketId: socket.id,
      });
    }
  });

  // Viewer Chat Message
  socket.on('chat:send-message', (data) => {
    const userId = data?.user_id || socket.id;
    const username = sanitizeText(data?.username || 'Viewer');
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
    if (Date.now() - lastSent < 400) {
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
      role: 'viewer',
      message: cleanMessage,
      created_at: new Date().toISOString(),
      is_deleted: false,
    };

    chatMessages.push(newMsg);
    if (chatMessages.length > 200) chatMessages.shift();

    io.to(streamState.id).emit('chat:new-message', newMsg);
    adminNs.emit('chat:new-message', newMsg);
  });

  // Disconnect
  socket.on('disconnect', () => {
    rateLimitMap.delete(socket.id);

    if (socket.data?.viewerId) {
      const streamViewers = viewersPresence.get(socket.data.streamId || streamState.id);
      if (streamViewers) {
        streamViewers.delete(socket.data.viewerId);
        broadcastViewerStats(io, adminNs, socket.data.streamId || streamState.id);
      }
    }
  });
});

server.listen(port, () => {
  console.log(`> StreamPulse Signaling & Realtime Server running on port ${port}`);
});
