use std::fs;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let num_nodes = &args[1];
    let from = &args[2];
    let to = &args[3];
    let base = sled::open(format!("logs/{}/server_1_logs/database/", from)).unwrap();

    for pid in 0..num_nodes.parse().unwrap() {
        for file in fs::read_dir(format!("logs/{}/server_1_logs/", from)).unwrap() {
            let file = file.unwrap();
            let path = file.path();
            if path.is_file() {
                fs::copy(
                    path,
                    format!(
                        "logs/{}/server_{}_logs/{}",
                        to,
                        pid + 1,
                        file.file_name().into_string().unwrap()
                    ),
                )
                .unwrap();
            }
        }

        let cur = sled::open(format!("logs/{}/server_{}_logs/database", to, pid + 1)).unwrap();
        let clone = base.export();
        for i in clone {
            let entries = i.2;
            for line in entries {
                let k = &line[0];
                let v = &line[1];
                let key = std::str::from_utf8(&k).unwrap();
                let value = std::str::from_utf8(&v).unwrap();
                cur.insert(key, value).unwrap();
                // let pair = format!("{{key: {}, value: {}}}, ", key, value);
                // pairs.push_str(&pair);
            }
        }
    }
}
