let encoder, pl, started = false, stopped = false;

self.addEventListener (
  'message',
  async function (e) {
    if (stopped) return;
    // In this demo, we expect at most two messages, one of each type.
    let type = e.data.type;

    if (type == 'stop') {
      //   self.postMessage ({text: 'Stop message received.'});
      if (started) pl.stop ();
      return;
    } else if (type != 'start') {
      self.postMessage ({severity: 'fatal', text: 'Invalid message received.'});
      return;
    }
    // We received a "stream" event
    // self.postMessage ({text: 'Stream event received.'});

    try {
      pl = new pipeline (e.data);
      pl.start ();
    } catch (e) {
      self.postMessage ({
        severity: 'fatal',
        text: `Pipeline creation failed: ${e.message}`,
      });
      return;
    }
  },
  false
);

class pipeline {
  constructor (eventData) {
    this.stopped = false;
    this.inputStream = eventData.input;
    this.config = eventData.config;
  }
  EncodeVideoStream (self, config) {
    return new TransformStream ({
      async start (controller) {
        this.frameCounter = 0;
        this.pending_outputs = 0;
        this.encoder = encoder = new VideoEncoder ({
          output: (chunk, cfg) => {
            // if (chunk.type != 'config') {
            //   const after = performance.now ();
            //   enc_update ({output: 1, timestamp: chunk.timestamp, time: after});
            // }
            // chunk.temporalLayerId = 0;
            // if (cfg.svc) {
            //   chunk.temporalLayerId = cfg.svc.temporalLayerId;
            // }
            // this.seqNo++;
            // if (chunk.type == 'key') {
            //   this.keyframeIndex++;
            //   this.deltaframeIndex = 0;
            // } else {
            //   this.deltaframeIndex++;
            // }
            this.pending_outputs--;
            // chunk.seqNo = this.seqNo;
            // chunk.keyframeIndex = this.keyframeIndex;
            // chunk.deltaframeIndex = this.deltaframeIndex;
            controller.enqueue (chunk);
          },
          error: e => {
            self.postMessage ({
              severity: 'fatal',
              text: `Encoder error: ${e.message}`,
            });
          },
        });
        try {
          const encoderSupport = await VideoEncoder.isConfigSupported (config);
          if (encoderSupport.supported) {
            this.encoder.configure (encoderSupport.config);
            // self.postMessage ({
            //   text: 'Encoder successfully configured:\n' +
            //     JSON.stringify (encoderSupport.config),
            // });
          } else {
            self.postMessage ({
              severity: 'fatal',
              text: 'Config not supported:\n' +
                JSON.stringify (encoderSupport.config),
            });
          }
        } catch (e) {
          self.postMessage ({
            severity: 'fatal',
            text: `Configuration error: ${e.message}`,
          });
        }
      },
      transform (frame, controller) {
        if (this.encoder.encodeQueueSize > 2) {
          frame.close ();
          return;
        }
        const keyFrame = this.frameCounter++ % config.keyInterval === 0;
        this.pending_outputs++;
        this.encoder.encode (frame, {keyFrame});
        frame.close ();
      },
      async flush () {
        await this.encoder.flush ();
        this.encoder.close ();
      },
    });
  }

  stop () {
    if (encoder.state != 'closed') encoder.close ();
    stopped = true;
    this.stopped = true;
    return;
  }

  async start () {
    if (stopped) return;
    started = true;
    let duplexStream, readStream, writeStream;
    try {
      await this.inputStream
        .pipeThrough (this.EncodeVideoStream (self, this.config))
        .pipeTo (
          new WritableStream ({
            write (chunk) {
              self.postMessage ({
                type: 'data',
                data: chunk,
              });
            },
          })
        );
    } catch (e) {
      self.postMessage ({severity: 'fatal', text: `start error: ${e.message}`});
    }
  }
}
