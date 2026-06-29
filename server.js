// signaling_server.js
const WebSocket = require('ws');

const clients = new Map();
const wss = new WebSocket.Server({ port: 8443 });

wss.on('connection', (ws) => {
    let clientId = null;

    ws.on('message', (raw) => {
        let data;
        try {
            data = JSON.parse(raw);
        } catch {
            console.error('Invalid JSON received');
            return;
        }

        if (data.type === 'register') {
            clientId = data.id;
            clients.set(clientId, ws);
            console.log(`Client ${clientId} registered`);

            // Notify other clients that a new peer has registered
            const notify = JSON.stringify({ type: 'peer-registered', id: clientId });
            for (const [cid, clientWs] of clients) {
                if (cid !== clientId && clientWs.readyState === WebSocket.OPEN) {
                    try {
                        clientWs.send(notify);
                    } catch (err) {
                        console.error(`Failed to notify ${cid}:`, err.message);
                    }
                }
            }

        } else if (['offer', 'answer', 'ice'].includes(data.type)) {
            const target = data.target;
            const targetWs = clients.get(target);
            if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                targetWs.send(raw.toString());
            } else {
                console.warn(`Target '${target}' not found or not connected`);
            }
        }
    });

    ws.on('close', () => {
        if (clientId && clients.has(clientId)) {
            clients.delete(clientId);
            console.log(`Client ${clientId} disconnected`);

            // Notify remaining clients that the peer has left
            const notify = JSON.stringify({ type: 'peer-left', id: clientId });
            for (const [, clientWs] of clients) {
                if (clientWs.readyState === WebSocket.OPEN) {
                    try {
                        clientWs.send(notify);
                    } catch (err) {
                        console.error('Failed to send peer-left:', err.message);
                    }
                }
            }
        }
    });

    ws.on('error', (err) => {
        console.error(`WebSocket error for client ${clientId}:`, err.message);
    });
});

console.log('Signaling server running on ws://0.0.0.0:8443');
