const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
    credentials: false
  },
  transports: ['polling', 'websocket'],
  allowUpgrades: true,
  pingTimeout: 30000,
  pingInterval: 10000
});

// =====================================================
// WORD LIST
// =====================================================
const WORD_LIST = [
  'CONSENSUS','VALIDATOR','GENLAYER','CONTRACT','TESTNET',
  'BRADBURY','FINALIZED','ACCEPTED','PYTHON','STAKING',
  'OPTIMISTIC','DELEGATED','APPEAL','GENVM','ORACLE',
  'SANDBOX','WEBHOOK','VOTING','ROLLUP','DISPATCH'
];

const GRID_SIZE = 15;
const GAME_DURATION = 300; // 5 minutes in seconds

const DIRECTIONS = [
  [0,1],[0,-1],[1,0],[-1,0],
  [1,1],[1,-1],[-1,1],[-1,-1]
];

// =====================================================
// IN-MEMORY ROOMS
// =====================================================
// rooms[code] = {
//   code, hostId, hostName, status, grid, placements,
//   players: { socketId: { name, wordsFound, lastWordAt, finishedAt, joinedAt } },
//   startedAt, endsAt, timerInterval
// }
const rooms = {};

// socketId -> roomCode (for disconnect cleanup)
const socketRoom = {};

// =====================================================
// GRID GENERATION
// =====================================================
function generateGrid(words) {
  const grid = Array.from({ length: GRID_SIZE }, () => Array(GRID_SIZE).fill(''));
  const placements = {};
  const shuffled = [...words].sort(() => Math.random() - 0.5);

  for (const word of shuffled) {
    let placed = false;
    let attempts = 0;
    while (!placed && attempts < 300) {
      attempts++;
      const [dr, dc] = DIRECTIONS[Math.floor(Math.random() * DIRECTIONS.length)];
      const r = Math.floor(Math.random() * GRID_SIZE);
      const c = Math.floor(Math.random() * GRID_SIZE);
      const cells = [];
      let valid = true;
      for (let i = 0; i < word.length; i++) {
        const nr = r + dr * i;
        const nc = c + dc * i;
        if (nr < 0 || nr >= GRID_SIZE || nc < 0 || nc >= GRID_SIZE) { valid = false; break; }
        if (grid[nr][nc] !== '' && grid[nr][nc] !== word[i]) { valid = false; break; }
        cells.push([nr, nc]);
      }
      if (!valid) continue;
      cells.forEach(([nr, nc], i) => { grid[nr][nc] = word[i]; });
      placements[word] = cells.map(([nr, nc]) => ({ r: nr, c: nc }));
      placed = true;
    }
  }

  const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  for (let r = 0; r < GRID_SIZE; r++)
    for (let c = 0; c < GRID_SIZE; c++)
      if (!grid[r][c]) grid[r][c] = letters[Math.floor(Math.random() * 26)];

  return { grid, placements };
}

function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from({ length: 4 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}

// =====================================================
// ROOM HELPERS
// =====================================================
function getPublicPlayers(room) {
  return Object.entries(room.players).map(([id, p]) => ({
    id,
    name: p.name,
    wordsFound: p.wordsFound.length,
    lastWordAt: p.lastWordAt,
    finishedAt: p.finishedAt,
    isHost: id === room.hostId
  }));
}

function endRoom(code, reason) {
  const room = rooms[code];
  if (!room) return;
  if (room.timerInterval) { clearInterval(room.timerInterval); room.timerInterval = null; }
  room.status = 'finished';

  const finalScores = getPublicPlayers(room).map(p => ({
    name: p.name,
    wordsFound: p.wordsFound,
    timeTaken: p.finishedAt
      ? Math.floor((p.finishedAt - room.startedAt) / 1000)
      : p.lastWordAt
        ? Math.floor((p.lastWordAt - room.startedAt) / 1000)
        : GAME_DURATION
  })).sort((a, b) => b.wordsFound - a.wordsFound || a.timeTaken - b.timeTaken);

  io.to(code).emit('game_ended', { reason, finalScores });

  // Clean up after 30s
  setTimeout(() => {
    if (rooms[code]) {
      Object.keys(rooms[code].players).forEach(sid => { delete socketRoom[sid]; });
      delete rooms[code];
    }
  }, 30000);
}

function isNameTaken(name) {
  for (const code in rooms) {
    const room = rooms[code];
    if (room.status === 'waiting') {
      for (const p of Object.values(room.players)) {
        if (p.name.toLowerCase() === name.toLowerCase()) return true;
      }
    }
  }
  return false;
}

// =====================================================
// SOCKET EVENTS
// =====================================================
io.on('connection', (socket) => {
  console.log('Connected:', socket.id);

  // ---------- CREATE ROOM ----------
  socket.on('create_room', ({ name }) => {
    if (!name || !name.trim()) {
      return socket.emit('error', { message: 'Enter your name first' });
    }
    name = name.trim().slice(0, 20);
    if (isNameTaken(name)) {
      return socket.emit('error', { message: 'Name already in use in an active room. Pick another.' });
    }

    let code;
    let attempts = 0;
    do { code = generateCode(); attempts++; } while (rooms[code] && attempts < 20);

    rooms[code] = {
      code,
      hostId: socket.id,
      hostName: name,
      status: 'waiting',
      grid: [],
      placements: {},
      players: {
        [socket.id]: {
          name,
          wordsFound: [],
          lastWordAt: null,
          finishedAt: null,
          joinedAt: Date.now()
        }
      },
      startedAt: null,
      endsAt: null,
      timerInterval: null
    };

    socketRoom[socket.id] = code;
    socket.join(code);

    socket.emit('room_created', {
      code,
      players: getPublicPlayers(rooms[code])
    });

    console.log(`Room ${code} created by ${name}`);
  });

  // ---------- JOIN ROOM ----------
  socket.on('join_room', ({ code, name }) => {
    code = (code || '').trim().toUpperCase();
    name = (name || '').trim().slice(0, 20);

    if (!code || code.length < 4) {
      return socket.emit('error', { message: 'Enter a valid room code' });
    }
    if (!name) {
      return socket.emit('error', { message: 'Enter your name' });
    }

    const room = rooms[code];
    if (!room) {
      return socket.emit('error', { message: 'Room not found. Check the code.' });
    }
    if (room.status === 'finished') {
      return socket.emit('error', { message: 'That game already ended' });
    }

    // Check name taken in this room
    for (const p of Object.values(room.players)) {
      if (p.name.toLowerCase() === name.toLowerCase()) {
        return socket.emit('error', { message: 'Name already taken in this room' });
      }
    }

    // Check name taken in other waiting rooms
    if (isNameTaken(name)) {
      return socket.emit('error', { message: 'Name already in use in another room. Pick another.' });
    }

    room.players[socket.id] = {
      name,
      wordsFound: [],
      lastWordAt: null,
      finishedAt: null,
      joinedAt: Date.now()
    };

    socketRoom[socket.id] = code;
    socket.join(code);

    const players = getPublicPlayers(room);

    // Tell joiner their state
    if (room.status === 'active') {
      socket.emit('room_joined', {
        code,
        status: 'active',
        grid: room.grid,
        endsAt: room.endsAt,
        players
      });
    } else {
      socket.emit('room_joined', {
        code,
        status: 'waiting',
        players
      });
    }

    // Tell everyone else
    io.to(code).emit('player_joined', { players });

    console.log(`${name} joined room ${code}`);
  });

  // ---------- START GAME ----------
  socket.on('start_game', () => {
    const code = socketRoom[socket.id];
    if (!code) return socket.emit('error', { message: 'Not in a room' });
    const room = rooms[code];
    if (!room) return socket.emit('error', { message: 'Room not found' });
    if (room.hostId !== socket.id) return socket.emit('error', { message: 'Only the host can start' });
    if (Object.keys(room.players).length < 2) {
      return socket.emit('error', { message: 'Need at least 2 players to start' });
    }
    if (room.status === 'active') return;

    const { grid, placements } = generateGrid(WORD_LIST);
    room.grid = grid;
    room.placements = placements;
    room.status = 'active';
    room.startedAt = Date.now();
    room.endsAt = Date.now() + GAME_DURATION * 1000;

    io.to(code).emit('game_started', {
      grid,
      endsAt: room.endsAt,
      players: getPublicPlayers(room)
    });

    // Server-side timer
    room.timerInterval = setInterval(() => {
      if (Date.now() >= room.endsAt) {
        clearInterval(room.timerInterval);
        room.timerInterval = null;
        endRoom(code, 'timeout');
      }
    }, 1000);

    console.log(`Game started in room ${code}`);
  });

  // ---------- WORD FOUND ----------
  socket.on('word_found', ({ word }) => {
    const code = socketRoom[socket.id];
    if (!code) return;
    const room = rooms[code];
    if (!room || room.status !== 'active') return;
    const player = room.players[socket.id];
    if (!player) return;
    if (!WORD_LIST.includes(word)) return;
    if (player.wordsFound.includes(word)) return;

    player.wordsFound.push(word);
    player.lastWordAt = Date.now();
    if (player.wordsFound.length === WORD_LIST.length) {
      player.finishedAt = Date.now();
    }

    // Broadcast updated scores to room
    io.to(code).emit('scores_updated', {
      players: getPublicPlayers(room)
    });

    // Check if all players finished
    const allDone = Object.values(room.players).every(p => p.finishedAt !== null);
    if (allDone) {
      clearInterval(room.timerInterval);
      room.timerInterval = null;
      endRoom(code, 'all_finished');
    }
  });

  // ---------- END GAME (host) ----------
  socket.on('end_game', () => {
    const code = socketRoom[socket.id];
    if (!code) return;
    const room = rooms[code];
    if (!room) return;
    if (room.hostId !== socket.id) return;
    endRoom(code, 'host_ended');
  });

  // ---------- END ROOM (host closes lobby) ----------
  socket.on('end_room', () => {
    const code = socketRoom[socket.id];
    if (!code) return;
    const room = rooms[code];
    if (!room) return;
    if (room.hostId !== socket.id) return;

    io.to(code).emit('room_closed', { message: 'Host closed the room' });

    // Clean up
    Object.keys(room.players).forEach(sid => { delete socketRoom[sid]; });
    delete rooms[code];
    console.log(`Room ${code} closed by host`);
  });

  // ---------- LEAVE ROOM ----------
  socket.on('leave_room', () => {
    handleLeave(socket);
  });

  // ---------- DISCONNECT ----------
  socket.on('disconnect', () => {
    console.log('Disconnected:', socket.id);
    handleLeave(socket);
  });

  function handleLeave(socket) {
    const code = socketRoom[socket.id];
    if (!code) return;
    const room = rooms[code];
    if (!room) { delete socketRoom[socket.id]; return; }

    const player = room.players[socket.id];
    const playerName = player ? player.name : 'Unknown';
    delete room.players[socket.id];
    delete socketRoom[socket.id];
    socket.leave(code);

    const remaining = Object.keys(room.players).length;

    // If host left and game not started, close room
    if (socket.id === room.hostId && room.status === 'waiting') {
      io.to(code).emit('room_closed', { message: 'Host left the room' });
      Object.keys(room.players).forEach(sid => { delete socketRoom[sid]; });
      delete rooms[code];
      console.log(`Room ${code} closed - host left`);
      return;
    }

    // If host left during active game, assign new host
    if (socket.id === room.hostId && room.status === 'active') {
      const newHostId = Object.keys(room.players)[0];
      if (newHostId) {
        room.hostId = newHostId;
        room.players[newHostId].isHost = true;
        io.to(code).emit('host_changed', { newHostName: room.players[newHostId].name });
      }
    }

    // No players left - clean up
    if (remaining === 0) {
      if (room.timerInterval) clearInterval(room.timerInterval);
      delete rooms[code];
      console.log(`Room ${code} empty - cleaned up`);
      return;
    }

    // Tell remaining players
    io.to(code).emit('player_left', {
      playerName,
      players: getPublicPlayers(room)
    });

    console.log(`${playerName} left room ${code}, ${remaining} remaining`);
  }
});

// =====================================================
// HEALTH CHECK
// =====================================================
app.get('/', (req, res) => {
  const roomCount = Object.keys(rooms).length;
  const playerCount = Object.values(rooms).reduce((sum, r) => sum + Object.keys(r.players).length, 0);
  res.json({
    status: 'ok',
    rooms: roomCount,
    players: playerCount,
    uptime: Math.floor(process.uptime()) + 's'
  });
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

// =====================================================
// START
// =====================================================
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`GenPuzzles server running on port ${PORT}`);
});
