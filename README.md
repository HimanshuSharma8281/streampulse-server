# 📡 StreamPulse Signaling & Realtime Server (`streampulse-server`)

Ultra-lightweight standalone Node.js & Socket.IO server for WebRTC signaling, live presence tracking, and community chat dispatch.

---

## 🚀 Deployment (Render / Railway / Fly.io / VPS)

### Render / Railway Setup
- Build Command: `npm install`
- Start Command: `node server.js`
- Environment Variables:
  - `PORT`: `4000` (or injected by hosting platform)
  - `ADMIN_PASSWORD`: Your private admin broadcaster secret

---

## 🧪 Endpoints

- `GET /health` — Health check
- `GET /api/stream/state` — Stream status & telemetry
- `WebSocket /` — Socket.IO WebRTC signaling, heartbeats, presence, and moderated chat
