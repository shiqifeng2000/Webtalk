  const separator = 0x77656274616C6Bn; // "webtalk" as a 64-bit BigInt
  const boxLen = 15;
  const start_time = Date.now();
  class BoxStreamParser {
    constructor(audioWritable, videoWritable) {
      this.buffer = new Uint8Array();
      this.state = -1;
      this.decoderMap = {
        'acnf': this.handleAudioConfig.bind(this),
        'vcnf': this.handleVideoConfig.bind(this),
        'afrm': this.handleAudioFrame.bind(this),
        'vfrm': this.handleVideoFrame.bind(this)
      };

      // WebCodec decoders
      this.videoConfig = null;
      this.audioConfig = null;

      // let video = document.createElement("video");
      // video.autoplay = true;
      // video.controls = true;
      // container.appendChild(video);
      // let audioTrackGenerator = new MediaStreamTrackGenerator({ kind: "audio" });
      // let videoTrackGenerator = new MediaStreamTrackGenerator({ kind: "video" });
      // let audioWriter = audioTrackGenerator.writable.getWriter();
      // let videoWriter = videoTrackGenerator.writable.getWriter();
      // video.srcObject = new MediaStream([videoTrackGenerator, audioTrackGenerator]);
      // video.onplay = () => {
      //   log(`timecost ${Date.now() - start_time}`)
      // }
      let audioWriter = audioWritable.getWriter();
      let videoWriter = videoWritable.getWriter();
      this.audioDecoder = new AudioDecoder({
        output: (frame) => {
          audioWriter.write(frame)
        },
        error: console.error
      });
      this.videoDecoder = new VideoDecoder({
        output: (frame) => {
          videoWriter.write(frame)
        },
        error: console.error
      });
      this.timestamp = 0n;
      this.videoTimestamp = 0n;
      this.debugLog('Parser initialized');
    }

    debugLog(msg) {
      // const debugDiv = document.getElementById('debug');
      // debugDiv.innerHTML += msg + "<br>";
      console.log("debug",msg);
    }

    updateStatus(status) {
      // document.getElementById('status').innerHTML = 'Status: ' + status;
      console.log("status",status);
    }

    // Parse incoming binary data
    parseChunk(chunk) {
      // Append new data to buffer
      const newBuffer = new Uint8Array(this.buffer.length + chunk.length);
      newBuffer.set(this.buffer);
      newBuffer.set(chunk, this.buffer.length);
      this.buffer = newBuffer;

      // Parse all complete boxes
      while (this.parseNextBox()) { }
    }

    parseNextBox() {
      // Need at least 9 bytes (separator + type + length)
      if (this.buffer.length < boxLen) return false;

      // Find separator (0x00)
      const separatorIndex = this.findSeparatorIndexFast();
      if (separatorIndex < 0) {
        // No separator found, discard buffer
        return false;
      }

      // Skip to separator position
      if (separatorIndex > 0) {
        this.debugLog(`Skipping ${separatorIndex} bytes to find separator`);
        this.buffer = this.buffer.slice(separatorIndex);
      }

      // Check if we have complete header
      if (this.buffer.length < boxLen) return false;

      // Parse header
      const typeBytes = this.buffer.slice(7, 11);
      const type = String.fromCharCode(...typeBytes);

      const lengthBytes = this.buffer.slice(11, 15);
      const payloadLength = new DataView(lengthBytes.buffer).getUint32(0, false);

      // Check if we have complete box
      const totalBoxLength = boxLen + payloadLength;
      if (this.buffer.length < totalBoxLength) return false;

      // Extract payload
      const payload = this.buffer.slice(boxLen, boxLen + payloadLength);

      // Process the box
      this.processBox(type, payload);

      // Remove processed box from buffer
      this.buffer = this.buffer.slice(totalBoxLength);

      return true;
    }
    findSeparatorIndexFast(startIndex = 0) {
      if (this.buffer.byteLength < 7) {
        return -2;
      }
      const view = new DataView(this.buffer.buffer);
      for (let i = startIndex; i <= this.buffer.byteLength - 7; i++) {
        // Read 8 bytes (we only need 7, but read 8 for comparison)
        const word = view.getBigUint64(i, false); // Big-endian

        // Shift right by 8 bits to compare 7 bytes
        if ((word >> 8n) === separator) {
          return i;
        }
      }
      return -1;
    }
    processBox(type, payload) {
      const handler = this.decoderMap[type];
      if (handler) {
        handler(payload);
      } else {
        this.debugLog(`Unknown box type: ${type}`);
      }
    }

    // Handle audio configuration
    async handleAudioConfig(payload) {
      const codecString = new TextDecoder().decode(payload);
      this.debugLog(`Audio config: ${codecString}`);

      // Configure audio decoder
      // this.audioDecoder = new AudioDecoder({
      //   output: this.handleDecodedAudio.bind(this),
      //   error: (e) => this.debugLog(`AudioDecoder error: ${e}`)
      // });

      this.audioConfig = {
        codec: codecString,
        sampleRate: 48000, // Adjust based on your stream
        numberOfChannels: 2,
        // Add other codec-specific parameters if needed
      };
      this.audioDecoder.configure(this.audioConfig);
    }

    // Handle video configuration
    async handleVideoConfig(payload) {
      const view = new DataView(payload.buffer);
      let cursor = 0;
      let width = view.getUint16(cursor);
      cursor += 2;
      let height = view.getUint16(cursor);
      cursor += 2;
      let codecLen = view.getUint32(cursor);
      cursor += 4;
      const codecString = new TextDecoder().decode(payload.subarray(cursor, cursor + codecLen));
      this.debugLog(`Video config: ${codecString}`);
      cursor += codecLen;
      let extradataLen = view.getUint32(cursor);
      cursor += 4;
      let extradata = payload.subarray(cursor, cursor + extradataLen);

      this.videoConfig = {
        codec: codecString,
        description: extradata,
        // Add codec-specific configuration
        codedWidth: width,
        codedHeight: height,
        // displayAspectWidth: 640,
        // displayAspectHeight: 360,
        // For H.264, you might need:
        // avc: { format: 'avc' }
      };
      // Configure video decoder
      // this.videoDecoder = new VideoDecoder({
      //   output: this.handleDecodedVideo.bind(this),
      //   error: (e) => this.debugLog(`VideoDecoder error: ${e}`)
      // });
      this.videoDecoder.configure(this.videoConfig);
    }

    // Handle audio frame
    handleAudioFrame(payload) {
      if (!this.audioDecoder || this.audioDecoder.state !== 'configured') {
        this.debugLog('Audio decoder not ready, queueing frame');
        // Queue frame for later processing
        // setTimeout(() => this.handleAudioFrame(payload), 100);
        return;
      }

      const view = new DataView(payload.buffer, payload.byteOffset);
      const timestamp = view.getUint32(0);
      // console.log("opus timestamp", timestamp);
      // Parse frame header (you might want timestamps)
      // Assuming payload is raw encoded audio data
      const chunk = new EncodedAudioChunk({
        type: 'key', // or 'delta' depending on your encoding
        // timestamp: performance.now() * 1000, // microseconds
        timestamp: -1,
        duration: 0,
        data: payload.slice(4)
      });

      this.audioDecoder.decode(chunk);
      // audio as standard timestamp
      this.timestamp += BigInt(timestamp * 1000 / this.audioConfig.sampleRate);
      // let duration = Date.now() - start_time;
      // console.log(`audio ${this.timestamp} - player ${duration} = ${this.timestamp - BigInt(duration)} `,);
    }

    // Handle video frame
    handleVideoFrame(payload) {
      if (!this.videoDecoder || this.videoDecoder.state !== 'configured') {
        this.debugLog('Video decoder not ready, queueing frame');
        // setTimeout(() => this.handleVideoFrame(payload), 100);
        return;
      }

      // Parse frame metadata if you embed it
      // For example: [timestamp: 8 bytes][is_keyframe: 1 byte][data: ...]
      const view = new DataView(payload.buffer, payload.byteOffset);
      // const timestamp = view.getBigUint64(0, false); // Big-endian
      const isKeyframe = view.getUint8(0) === 1;
      const timestamp = view.getUint32(1);

      let nalLen = payload.byteLength - 5;
      const newBuffer = new Uint8Array(nalLen + 4);
      const newView = new DataView(newBuffer.buffer, 0);
      newView.setUint32(0, nalLen, false);
      newBuffer.set(payload.slice(5), 4);
      // const frameData = payload.slice(1);

      if (!isKeyframe && this.state === -1) {
        return;
      }
      const chunk = new EncodedVideoChunk({
        type: isKeyframe ? 'key' : 'delta',
        // timestamp: Number(timestamp),
        timestamp: -1,
        data: newBuffer
      });
      this.videoDecoder.decode(chunk);
      if (this.state === -1) {
        this.state = 0;
      }
      this.videoTimestamp += BigInt(timestamp * 1000 / 90000);
      // console.log(`audio ${this.timestamp} - video ${this.videoTimestamp} = ${this.timestamp - this.videoTimestamp} `,);
      let duration = Date.now() - start_time;
      // console.log(`video ${this.videoTimestamp} - player ${duration} = ${this.videoTimestamp - BigInt(duration)}, audio ${this.timestamp} - player ${duration} = ${this.timestamp - BigInt(duration)} `,);
      // console.log(`video ${this.videoTimestamp} - player ${duration} = ${this.videoTimestamp - BigInt(duration)} `,);
    }

    // Cleanup
    destroy() {
      if (this.videoDecoder) this.videoDecoder.close();
      if (this.audioDecoder) this.audioDecoder.close();
      this.debugLog('Parser destroyed');
    }
  }

  class StreamFetcher {
    constructor(url, audioWritable, videoWritable) {
      this.url = url;
      this.parser = new BoxStreamParser(audioWritable, videoWritable);
      this.isStreaming = false;
    }

    async start() {
      try {
        this.isStreaming = true;
        this.parser.updateStatus('Connecting...');

        const response = await fetch(this.url);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const reader = response.body.getReader();
        this.parser.updateStatus('Streaming...');

        while (this.isStreaming) {
          const { done, value } = await reader.read();

          if (done) {
            this.parser.updateStatus('Stream ended');
            break;
          }

          // Parse the chunk
          this.parser.parseChunk(value);
        }

      } catch (error) {
        this.parser.updateStatus(`Error: ${error.message}`);
        this.parser.debugLog(`Fetch error: ${error}`);
      }
    }

    stop() {
      this.isStreaming = false;
      this.parser.updateStatus('Stopped');
      this.parser.destroy();
    }
  }

onmessage = async (data)=>{
  let {url, audioWritable, videoWritable, type} = data.data;
  if (type === "start") {
    if (self.streamFetcher) {
      self.streamFetcher.stop();
    }
    self.streamFetcher = new StreamFetcher(url, audioWritable, videoWritable);  
    await self.streamFetcher.start();
  } else if (type === "stop") {
    if (self.streamFetcher) {
      self.streamFetcher.stop();
    }
  }
}