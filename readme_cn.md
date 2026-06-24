### 基于webrtc/rtmp技术的流媒体服务
目前有基于中枢转发(SFU)和点对点对传(P2P)两种模式

#### http分发
为了便于拉流端拉流，增加了es模式用于webrtc和http的户转

1. http拉流

http拉流是把webrtc推上去的流解封装成为原始音视频的方法，因为http流的特性，这里添加了类似post formdata的separator功能，设计如下

音视频是由一个一个的esbox组成，类似简化版mp4 box，只有4种box, acnf, afrm, vcnf, vfrm
地址为 host/es_streamer/{target}?query...

用户请求时可以传自定义分割符如sep=mywebtalk，也就是 host/es_streamer/{target}?sep=mywebtalk，如果不传默认为webtalk

拉流时，response作为读出流，会由如下一个个box组成

##### acnf
```
--------------------------------------------------------------------------------------------
|   SEP(user defined)   |  BoxName(acnf)  |PayloadLen(uint32)|       Payload(Rfc6381)      |
--------------------------------------------------------------------------------------------      
```
##### afrm
```
-------------------------------------------------------------------------------------------------
|   SEP(user defined)   |  BoxName(afrm)  |PayloadLen(uint32)|Duration(uint32)|Payload(opus/aac)|
------------------------------------------------------------------------------------------------- 
```
##### vcnf
Payload包含了width, height, rfc6381codec, avccExtradata，如果解码器需要annexb，那可以根据Extradata定义将sps/pps解析出来
```
--------------------------------------------------------------------------------------------
|   SEP(user defined)   |  BoxName(vcnf)  |PayloadLen(uint32)|     Payload(DetailBelow)    |
-------------------------------------------------------------------------------------------- 
Payload:
--------------------------------------------------------------------------------------------
|Width(uint16)|Height(uint16)|Rfc6381Len(uint32)|Rfc6381|ExtraDataLen(uint32)|AvccExtraData|
-------------------------------------------------------------------------------------------- 
```
H264 Avcc Extradata Definition
![Definition](static/avcc_extradata.png)

##### vfrm

Payload只是去掉annexb prefix的Nal，如果是avcc格式的解码器需要在进入解码器前自行添加uint32的payloadLen，如果是annexb的化可以直接添加annexb prefix(0x00 0x00 0x00 0x01)

```
------------------------------------------------------------------------------------------------------
|   SEP(user defined)   |  BoxName(vfrm)  |PayloadLen(uint32)+4+1|isKey(u8)|Duration(uint32)|Payload(Nal)|
------------------------------------------------------------------------------------------------------ 
```


2. http会议

http会议是以http2.0为基础，进行网页推拉流

音视频是由一个一个的esbox组成，类似简化版mp4 box，只有4种box, acnf, afrm, vcnf, vfrm
地址为 host/es_streamer/{target}?query...

用户请求时可以传自定义分割符如sep=mywebtalk，也就是 host/es_streamer/{target}?sep=mywebtalk，如果不传默认为webtalk

拉流时，response作为读出流，会由如下一个个box组成

##### acnf
```
--------------------------------------------------------------------------------------------
|   SEP(user defined)   |  BoxName(acnf)  |PayloadLen(uint32)|       Payload(Rfc6381)      |
--------------------------------------------------------------------------------------------      
```
##### afrm
```
-------------------------------------------------------------------------------------------------
|   SEP(user defined)   |  BoxName(afrm)  |PayloadLen(uint32)|Duration(uint32)|Payload(opus/aac)|
------------------------------------------------------------------------------------------------- 
```
##### vcnf
Payload包含了width, height, rfc6381codec, avccExtradata，如果解码器需要annexb，那可以根据Extradata定义将sps/pps解析出来
```
--------------------------------------------------------------------------------------------
|   SEP(user defined)   |  BoxName(vcnf)  |PayloadLen(uint32)|     Payload(DetailBelow)    |
-------------------------------------------------------------------------------------------- 
Payload:
--------------------------------------------------------------------------------------------
|Width(uint16)|Height(uint16)|Rfc6381Len(uint32)|Rfc6381|ExtraDataLen(uint32)|AvccExtraData|
-------------------------------------------------------------------------------------------- 
```
H264 Avcc Extradata Definition
![Definition](static/avcc_extradata.png)

##### vfrm

Payload只是去掉annexb prefix的Nal，如果是avcc格式的解码器需要在进入解码器前自行添加uint32的payloadLen，如果是annexb的化可以直接添加annexb prefix(0x00 0x00 0x00 0x01)

```
------------------------------------------------------------------------------------------------------
|   SEP(user defined)   |  BoxName(vfrm)  |PayloadLen(uint32)|isKey(u8)|Duration(uint32)|Payload(Nal)|
------------------------------------------------------------------------------------------------------ 
```


#### webrtc部分  

1. 中枢转发

中枢转发是以http接口方式交换信令,也就是推流(WHIP)和拉流(WHEP),在本项目开发阶段,服务器端口由于比较紧缺,导致设计上我们分为贪婪模式/省端口模式, 两种模式下接口设计都是一样的,不同的是,贪婪模式下webrtc端口会根据自动终端网卡数开很多端口,节省模式下会根据客户端申请的端口数尽可能少地开放端口


以下时信令交换的http接口

PATH:       /publish  
METHOD:     POST  
COMMENT:    推流接口
PAYLOAD:    
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    token        usigned int           y             推流任务号,全局唯一,如果为空本地会随机生成   
    sockets      unsigne int           y             申请的端口数,默认一次连接一个端口,如果需要可以额外申请更多端口,多申请端口会提升webrtc连接的稳定性
    peer         String                n             base64形式的信令/sdp,必填
```
RETURN:
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    success         bool               n                是否成功
    sid         unsigned int           n                任务号
    data           String              n                base64形式的信令/sdp
```

PATH:       /subscribe  
METHOD:     POST  
COMMENT:    拉流接口
PAYLOAD:    
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    target       usigned int           n             拉流任务号,必填   
    sockets      unsigne int           y             申请的端口数,默认一次连接一个端口,如果需要可以额外申请更多端口,多申请端口会提升webrtc连接的稳定性
    peer         String                n             base64形式的信令/sdp,必填

```
RETURN:
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    success         bool               n                是否成功
    data           String              n                base64形式的信令/sdp

```

PATH:       /publish_subscribe  
METHOD:     POST  
COMMENT:    既推又拉接口
PAYLOAD:    
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    token        usigned int           y             推流任务号,全局唯一,如果为空本地会随机生成   
    target       usigned int           n             拉流任务号,必填   
    sockets      unsigne int           y             申请的端口数,默认一次连接一个端口,如果需要可以额外申请更多端口,多申请端口会提升webrtc连接的稳定性
    peer         String                n             base64形式的信令/sdp,必填

```
RETURN:
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    success         bool               n                是否成功
    sid         unsigned int           n                任务号
    data           String              n                base64形式的信令/sdp

```

2. 点对点模式

点对点是以websocket方式交换信令, 支持多个终端之间直接点对点通信, 服务端不负责转发工作,websocket需要交换信令和candidate,以下是websocket信息格式,需注意一个websocket连接下, 一次只能申请一个webrtc连接

Sdp 消息:
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    sid         unsigned int           y            用于应答时提供的会话号,会话为一对一组,全局唯一
    type           String              n            消息类型,分为 offer/answer两种
    sdp            String              n            webrtc信令/sdp

```
Candidate 消息:
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    type           String              n            消息类型, 值为iceCandidate
    candidate      Candidate(struct)   y            candiate结构,用于对方设置远端设置iceCandidate所用,如果为空则表示本地所有端口已收集

```
Candidate结构体:
```
-----------------------------------------------------------------------------------------
|   FIELD   |       |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    candidate          String              n            具体开通的udp端口的参数
    sdpMid             String              y            sdp的media index参数
    sdpMlineIndex      usigned short       y            sdp的mline位置
    usernameFrgment    String              y            sdp的ufrag配置,唯一

```
StopTalk 消息:
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    type           String              n            消息类型,值为stopTalk
    sid            String              n            会话号,全局唯一
```


3. 管理端口

PATH:       /sessions  
METHOD:     GET  
COMMENT:    查看中枢转发流程中的会话
RETURN:  

```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    success         bool               n                是否成功
    sessions   HashMap<id,RtcSession>  n                NFU会话列表

```
RtcSession:
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    id          unsigned int           n            任务号,全局唯一
    sockets   list<unsigned int>       n            节省模式下所使用的端口
    listener    list<String>           n            拉流方,默认为终端的socket地址
    elipsed     unsigned long          n            会话创建后经过的时间,单位为秒           

```



PATH:       /p2p_sessions  
METHOD:     GET  
COMMENT:    查看点对点流程中的会话, 待定



4. STUN/TURN 服务  

目前服务自带stun/turn服务, 可参考环境变量:
```
PEER_STUN_ADDRS=10.10.87.239:9013
STUN_ADDR=0.0.0.0:9013
TURN_ADDR=0.0.0.0:9012
TURN_USERS="robin=12345"
```
其中
PEER_STUN_ADDRS是服务处于容器内时,指定对外暴露的地址
STUN_ADDR是STUN服务socket地址
TURN_ADDR是TURN服务socket地址
TURN_USERS是TURN服务的用户名密码



5. 和业务系统的对接  
设计上需要通过任务号和MQ进行对接,暂定MQ topic为boeshare_stream和boeshare_listener两个, key值用up表示推流方已推流/拉流方已拉流, down表示推流方已断流/拉流方已断开, unstable表示推流方socket有断开现象/拉流方socket有断开现象

- boeshare_stream
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    token       unsigned int           n                任务号
```

- boeshare_listener
```
-----------------------------------------------------------------------------------------
|   FIELD   |   |   TYPE    |   |   NULLABLE    |   |   COMMENT    |
-----------------------------------------------------------------------------------------    
    token       unsigned int           n                任务号
    addr        String                 n                暂定客户端http访问时用到的socket地址
```



#### rtmp部分
可以将webrtc和rtmp数据相互转化,但功能待定


./build.sh && zip webtalk.zip webtalk && scp -P 17822 ./webtalk.zip root@47.92.124.142:/data/webtalk/ && ssh -p 17822 root@47.92.124.142

### fmp4
mdat avcc 格式 len = nal_len + 4