use std::net::UdpSocket;
use std::time::Duration;
use std::{env, process};

// 错误消息生成器
struct ErrorGenerator {
    target_addr: String,
    delay_ms: u64,
    repeat: usize,
}

impl ErrorGenerator {
    fn new(target_addr: &str, delay_ms: u64, repeat: usize) -> Self {
        Self {
            target_addr: target_addr.to_string(),
            delay_ms,
            repeat,
        }
    }

    // 发送错误消息
    fn send_errors(&self) -> std::io::Result<()> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_write_timeout(Some(Duration::from_secs(1)))?;

        println!("Starting error message flood to {}", self.target_addr);
        println!("Delay: {}ms, Repeat: {} times", self.delay_ms, self.repeat);

        // 创建各种类型的错误消息
        let error_messages = self.generate_error_messages();

        for i in 0..self.repeat {
            for (desc, data) in &error_messages {
                if let Err(e) = socket.send_to(data, &self.target_addr) {
                    eprintln!("[!] Failed to send '{}': {}", desc, e);
                } else {
                    println!(
                        "[{}/{}] Sent '{}' ({} bytes) to {}",
                        i + 1,
                        self.repeat,
                        desc,
                        data.len(),
                        self.target_addr
                    );
                }

                // 延迟
                std::thread::sleep(Duration::from_millis(self.delay_ms));
            }
        }

        println!("Finished sending {} error batches", self.repeat);
        Ok(())
    }

    // 生成各种类型的错误消息
    fn generate_error_messages(&self) -> Vec<(String, Vec<u8>)> {
        vec![
            // 1. 空消息
            ("Empty message".to_string(), vec![]),
            // 2. 最小长度无效消息
            ("Minimal invalid".to_string(), vec![0x00; 4]),
            // 3. 随机垃圾数据
            (
                "Random garbage".to_string(),
                vec![
                    0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x11, 0x22, 0x33, 0x44, 0x55,
                    0x66, 0x77, 0x88,
                ],
            ),
            // 4. 类似STUN但魔术字错误
            (
                "Invalid magic cookie".to_string(),
                vec![
                    0x00, 0x01, 0x00, 0x08, // STUN头
                    0xAA, 0xBB, 0xCC, 0xDD, // 错误的Magic Cookie
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // 事务ID
                    0x08, 0x09, 0x0A, 0x0B, // 事务ID结尾
                ],
            ),
            // 5. 长度字段溢出
            (
                "Length overflow".to_string(),
                vec![
                    0x00, 0x01, 0xFF, 0xFF, // 长度65535
                    0x21, 0x12, 0xA4, 0x42, // Magic Cookie
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // 事务ID
                    0x08, 0x09, 0x0A, 0x0B, // 事务ID结尾
                ],
            ),
            // 6. 无效属性
            (
                "Invalid attribute".to_string(),
                vec![
                    0x00, 0x01, 0x00, 0x04, // STUN头
                    0x21, 0x12, 0xA4, 0x42, // Magic Cookie
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // 事务ID
                    0x08, 0x09, 0x0A, 0x0B, // 事务ID结尾
                    0xFF, 0xFF, 0x00, 0x04, // 无效属性类型
                    0x00, 0x00, 0x00, 0x00, // 属性值
                ],
            ),
            // 7. 超大消息 (超过MTU)
            ("Oversized message".to_string(), vec![0xAA; 4096]),
            // 8. 非法消息类
            (
                "Invalid message class".to_string(),
                vec![
                    0b11000000, 0x01, 0x00, 0x00, // 无效类 (0xC0)
                    0x21, 0x12, 0xA4, 0x42, // Magic Cookie
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // 事务ID
                    0x08, 0x09, 0x0A, 0x0B, // 事务ID结尾
                ],
            ),
            // 9. 部分有效消息
            (
                "Partial valid message".to_string(),
                vec![
                    0x00, 0x01, 0x00, 0x08, // STUN头
                    0x21, 0x12, 0xA4, 0x42, // Magic Cookie
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // 事务ID
                ], // 缺少事务ID结尾
            ),
            // 10. 高负载消息
            (
                "High-entropy data".to_string(),
                (0..1024).map(|_| rand::random::<u8>()).collect(),
            ),
        ]
    }
}

// 帮助信息
fn print_usage() {
    println!("UDP Error Message Flood Tool");
    println!("Usage:");
    println!("  udp-error-flood <target_ip:port> [options]");
    println!();
    println!("Options:");
    println!("  -d, --delay <ms>    Delay between messages in milliseconds (default: 100)");
    println!("  -r, --repeat <n>     Number of times to repeat the error sequence (default: 1)");
    println!("  -h, --help          Show this help message");
    println!();
    println!("Examples:");
    println!("  udp-error-flood 192.168.1.100:3478");
    println!("  udp-error-flood 10.0.0.5:1234 -d 50 -r 10");
    println!("  udp-error-flood localhost:8080 --delay 200 --repeat 5");
}

fn main() {
    // 获取命令行参数
    let args: Vec<String> = env::args().collect();

    // 显示帮助信息
    if args.len() < 2 || args.iter().any(|a| a == "-h" || a == "--help") {
        print_usage();
        process::exit(0);
    }

    // 解析目标地址
    let target_addr = args[1].clone();

    // 默认参数
    let mut delay_ms = 100;
    let mut repeat = 1;

    // 解析选项
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "-d" | "--delay" => {
                if i + 1 < args.len() {
                    delay_ms = args[i + 1].parse().unwrap_or(100);
                    i += 1;
                }
            }
            "-r" | "--repeat" => {
                if i + 1 < args.len() {
                    repeat = args[i + 1].parse().unwrap_or(1);
                    i += 1;
                }
            }
            _ => {
                eprintln!("Unknown option: {}", args[i]);
                print_usage();
                process::exit(1);
            }
        }
        i += 1;
    }

    // 创建错误生成器
    let generator = ErrorGenerator::new(&target_addr, delay_ms, repeat);

    // 发送错误消息
    if let Err(e) = generator.send_errors() {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
