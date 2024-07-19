use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Slot {
    addr: [u8; 20],
    index: [u8; 32],
}

#[derive(Deserialize, Debug)]
struct Tx {
    txid: [u8; 32],
    rdo_addr_list: Vec<[u8; 20]>,
    rnw_addr_list: Vec<[u8; 20]>,
    rdo_slot_list: Vec<Slot>,
    rnw_slot_list: Vec<Slot>,
}

#[derive(Deserialize, Debug)]
struct Block {
    height: i64,
    tx_list: Vec<Tx>,
}

#[derive(Deserialize, Debug)]
struct User {
    fingerprint: String,
    location: String,
}

fn main() {
    // The type of `j` is `&str`
    let j = "
        {
            \"fingerprint\": \"0xF9BA143B95FF6D82\",
            \"location\": \"Menlo Park, CA\"
        }";

    let u: User = serde_json::from_str(j).unwrap();
    println!("{:#?}", u);
}
