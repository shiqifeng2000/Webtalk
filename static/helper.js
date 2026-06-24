function stringToArrayBuffer (str) {
  const encoder = new TextEncoder ();
  const uint8Array = encoder.encode (str);
  return uint8Array;
}
function genBox (token, ssrc, boxName, payload) {
  let tokenBytes = stringToArrayBuffer (token);
  let boxNameBytes = stringToArrayBuffer (boxName);
  const buffer = new ArrayBuffer (
    payload.byteLength + tokenBytes.byteLength + boxNameBytes.byteLength + 4 + 4
  );
  const view = new DataView (buffer);
  const uint8Buffer = new Uint8Array (buffer);
  let cursor = 0;

  uint8Buffer.set (tokenBytes, cursor);
  cursor += tokenBytes.byteLength;

  view.setUint32 (cursor, ssrc, false);
  cursor += 4;

  uint8Buffer.set (boxNameBytes, cursor);
  cursor += boxNameBytes.byteLength;

  view.setUint32 (cursor, payload.byteLength, false);
  cursor += 4;

  uint8Buffer.set (payload, cursor);
  return uint8Buffer;
}

function genVideoConfiguration (token, ssrc, width, height, rfc6381) {
  let tokenBytes = stringToArrayBuffer (token);
  let boxNameBytes = stringToArrayBuffer ('vcnf');
  let rfc6381Bytes = stringToArrayBuffer (rfc6381);
  let payloadLen = 2 + 2 + 4 + rfc6381Bytes.byteLength;
  const buffer = new ArrayBuffer (
    tokenBytes.byteLength + 4 + boxNameBytes.byteLength + 4 + payloadLen
  );
  const view = new DataView (buffer);
  const uint8Buffer = new Uint8Array (buffer);
  let cursor = 0;

  uint8Buffer.set (tokenBytes, cursor);
  cursor += tokenBytes.byteLength;

  view.setUint32 (cursor, ssrc, false);
  cursor += 4;

  uint8Buffer.set (boxNameBytes, cursor);
  cursor += boxNameBytes.byteLength;

  view.setUint32 (cursor, payloadLen, false);
  cursor += 4;

  view.setUint16 (cursor, width, false);
  cursor += 2;

  view.setUint16 (cursor, height, false);
  cursor += 2;

  view.setUint32 (cursor, rfc6381Bytes.byteLength, false);
  cursor += 4;

  uint8Buffer.set (rfc6381Bytes, cursor);
  
  return uint8Buffer;
}

function genAudioConfiguration (token, ssrc, sampleRate, channels, rfc6381) {
  let tokenBytes = stringToArrayBuffer (token);
  let boxNameBytes = stringToArrayBuffer ('acnf');
  let rfc6381Bytes = stringToArrayBuffer (rfc6381);
  let payloadLen = 2 + 1 + 4 + rfc6381Bytes.byteLength;
  const buffer = new ArrayBuffer (
    tokenBytes.byteLength + 4 + boxNameBytes.byteLength + 4 + payloadLen
  );
  const view = new DataView (buffer);
  const uint8Buffer = new Uint8Array (buffer);
  let cursor = 0;

  uint8Buffer.set (tokenBytes, cursor);
  cursor += tokenBytes.byteLength;

  view.setUint32 (cursor, ssrc, false);
  cursor += 4;

  uint8Buffer.set (boxNameBytes, cursor);
  cursor += boxNameBytes.byteLength;

  view.setUint32 (cursor, payloadLen, false);
  cursor += 4;

  view.setUint16 (cursor, sampleRate, false);
  cursor += 2;

  view.setUint8 (cursor, channels, false);
  cursor += 1;

  view.setUint32 (cursor, rfc6381Bytes.byteLength, false);
  cursor += 4;

  uint8Buffer.set (rfc6381Bytes, cursor);
  return uint8Buffer;
}

function uint8ToBase64(uint8) {
  let binary = '';
  const chunkSize = 0x8000; // avoid call stack overflow

  for (let i = 0; i < uint8.length; i += chunkSize) {
    const sub = uint8.subarray(i, i + chunkSize);
    binary += String.fromCharCode(...sub);
  }

  return btoa(binary);
}