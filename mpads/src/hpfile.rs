use crate::def::PRE_READ_BUF_SIZE;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::{self, remove_file, File};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::RwLock;
use std::{error, io};

#[derive(Debug)]
pub struct HPFile {
    dir_name: String,
    block_size: i64,
    buffer_size: i64,
    file_map: RwLock<HashMap<i64, File>>,
    largest_id: AtomicI64,
    latest_file_size: AtomicI64,
}

impl HPFile {
    pub fn new(buffer_size: i64, block_size: i64, dir_name: String) -> Result<HPFile> {
        if block_size % buffer_size != 0 {
            panic!(
                "Invalid blockSize:{} bufferSize:{}",
                block_size, buffer_size
            );
        };

        let (id_list, largest_id) = Self::get_file_ids(&dir_name, block_size)?;
        let (file_map, latest_file_size) =
            Self::load_file_map(&dir_name, block_size, id_list, largest_id)?;

        Ok(HPFile {
            dir_name: dir_name.clone(),
            block_size,
            buffer_size,
            file_map: RwLock::new(file_map),
            largest_id: AtomicI64::new(largest_id),
            latest_file_size: AtomicI64::new(latest_file_size),
        })
    }

    fn get_file_ids(dir_name: &str, block_size: i64) -> Result<(Vec<i64>, i64)> {
        let mut largest_id = 0;
        let mut id_list: Vec<i64> = vec![];

        for entry in fs::read_dir(&dir_name)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                continue;
            }

            let id = Self::parse_filename(block_size, entry.file_name().to_str().unwrap())?;
            if largest_id < id {
                largest_id = id;
            }
            id_list.push(id);
        }

        Ok((id_list, largest_id))
    }

    fn parse_filename(block_size: i64, file_name: &str) -> Result<i64> {
        let two_parts: Vec<_> = file_name.split("-").collect();
        if two_parts.len() != 2 {
            return Err(anyhow!(
                "{} does not match the pattern 'FileId-BlockSize'",
                file_name
            )
            .into());
        };

        let id;
        match two_parts[0].parse::<i64>() {
            Ok(n) => id = n,
            Err(e) => return Err(e.into()),
        };

        let size;
        match two_parts[1].parse::<i64>() {
            Ok(n) => size = n,
            Err(_) => return Err(anyhow!("Invalid Filename: {}", file_name).into()),
        };

        if block_size != size {
            return Err(anyhow!("Invalid Size! {}!={}", size, block_size).into());
        }

        Ok(id)
    }

    fn load_file_map(
        dir_name: &str,
        block_size: i64,
        id_list: Vec<i64>,
        largest_id: i64,
    ) -> Result<(HashMap<i64, File>, i64)> {
        let mut file_map = HashMap::<i64, File>::new();
        let mut latest_file_size: i64 = 0;

        for &id in &id_list {
            let file_name = format!("{}/{}-{}", &dir_name, id, block_size);
            let file = File::options().read(true).write(true).open(file_name)?;
            file_map.insert(id, file);
            if id == largest_id {
                latest_file_size = file_map.get(&id).unwrap().seek(SeekFrom::End(0))? as i64;
            }
        }
        if id_list.is_empty() {
            let file_name = format!("{}/{}-{}", &dir_name, 0, block_size);
            let file = File::create_new(file_name)?;
            file_map.insert(0, file);
        };

        Ok((file_map, latest_file_size))
    }

    pub fn size(&self) -> i64 {
        self.largest_id.load(Ordering::SeqCst) * self.block_size
            + self.latest_file_size.load(Ordering::SeqCst)
    }

    pub fn truncate(&self, mut size: i64) -> Option<io::Error> {
        let mut file_map = self.file_map.write().unwrap();
        let mut largest_id = self.largest_id.load(Ordering::SeqCst);
        while size < largest_id * self.block_size {
            let _ = file_map.remove(&largest_id)?;
            let file_name = format!("{}/{}-{}", self.dir_name, largest_id, self.block_size);
            if let Err(err) = remove_file(file_name) {
                return Some(err);
            }
            self.largest_id.fetch_sub(1, Ordering::SeqCst);
            largest_id -= 1;
        }
        size -= largest_id * self.block_size;
        let _ = file_map.remove(&largest_id);
        let file_name = format!("{}/{}-{}", self.dir_name, largest_id, self.block_size);
        let f = match File::options().read(true).write(true).open(file_name) {
            Ok(file) => file,
            Err(err) => return Some(err),
        };
        file_map.insert(largest_id, f);
        self.latest_file_size.store(size, Ordering::SeqCst);
        let mut f = file_map.get(&largest_id)?;
        if let Err(err) = f.set_len(size as u64) {
            return Some(err);
        }
        f.seek(SeekFrom::End(0)).err()
    }

    pub fn flush(&self, buffer: &mut Vec<u8>) {
        let file_map = self.file_map.write().unwrap();
        let largest_id = self.largest_id.load(Ordering::SeqCst);
        let mut f = file_map.get(&largest_id).unwrap();
        if buffer.len() != 0 {
            f.seek(SeekFrom::End(0)).unwrap();
            f.write(&buffer).unwrap();
            buffer.clear();
        }
        let _ = f.sync_all();
    }

    pub fn close(&self) {
        let mut file_map = self.file_map.write().unwrap();
        file_map.clear();
    }

    pub fn read_at(&self, buf: &mut [u8], off: i64) -> usize {
        let file_id = off / self.block_size;
        let pos = off % self.block_size;
        let file_map = self.file_map.read().unwrap();
        let f = match file_map.get(&file_id) {
            None => {
                panic!(
                    "Can not find the file with id={} ({}/{})",
                    file_id, off, self.block_size
                );
            }
            Some(file) => file,
        };
        let r = f.read_at(buf, pos as u64).unwrap(); //return the read bytes number
        r
    }

    pub fn read_at_with_pre_reader(
        &self,
        buf: &mut Vec<u8>,
        num_bytes: usize,
        off: i64,
        pre_reader: &mut PreReader,
    ) {
        if buf.len() < num_bytes {
            buf.resize(num_bytes, 0);
        }
        let file_id = off / self.block_size;
        let pos = off % self.block_size;
        if pre_reader.try_read(file_id, pos, &mut buf[0..num_bytes]) {
            return;
        }
        let file_map = self.file_map.read().unwrap();
        let f = match file_map.get(&file_id) {
            None => {
                panic!(
                    "Can not find the file with id={} ({}/{})",
                    file_id, off, self.block_size
                );
            }
            Some(file) => file,
        };
        if num_bytes >= PRE_READ_BUF_SIZE || pos + num_bytes as i64 > self.block_size {
            if let Err(err) = f.read_at(buf, pos as u64) {
                panic!("file read error: {}", err);
            };
            return;
        }
        let f = file_map.get(&file_id).unwrap();
        pre_reader.fill_slice(file_id, pos, |slice: &mut [u8]| -> i64 {
            match f.read_at(slice, pos as u64) {
                Ok(n) => {
                    return n as i64;
                }
                Err(e) => {
                    panic!("{}", e);
                }
            }
        });
        let ok = pre_reader.try_read(file_id, pos, &mut buf[0..num_bytes]);
        if !ok {
            panic!(
                "Cannot read data just fetched in {} fileID {}",
                self.dir_name, file_id
            );
        }
    }

    pub fn append(&self, bz: &[u8], buffer: &mut Vec<u8>) -> i64 {
        let mut largest_id = self.largest_id.load(Ordering::SeqCst);
        let start_pos = self.size();
        if bz.len() as i64 > self.buffer_size {
            panic!("bz is too large");
        }
        let old_size = self
            .latest_file_size
            .fetch_add(bz.len() as i64, Ordering::SeqCst);
        let mut split_pos = 0;
        let extra_bytes = (buffer.len() + bz.len()) as i64 - self.buffer_size;
        if extra_bytes > 0 {
            // flush buffer_size bytes to disk
            split_pos = bz.len() - extra_bytes as usize;
            buffer.extend_from_slice(&bz[0..split_pos]);
            let file_map = self.file_map.read().unwrap();
            let mut f = file_map.get(&largest_id).unwrap();
            if let Err(_) = f.write(buffer.as_slice()) {
                panic!("Fail to write file");
            }
            buffer.clear();
        }
        buffer.extend_from_slice(&bz[split_pos..]); //put remained bytes into buffer
        let overflow_byte_count = old_size + bz.len() as i64 - self.block_size;
        if overflow_byte_count >= 0 {
            self.flush(buffer); // flush buffer's remained data out
            self.largest_id.fetch_add(1, Ordering::SeqCst);
            largest_id += 1;
            let file_name = format!("{}/{}-{}", self.dir_name, largest_id, self.block_size);
            let f = match File::create_new(&file_name) {
                Ok(file) => file,
                Err(_) => File::options()
                    .read(true)
                    .write(true)
                    .open(&file_name)
                    .unwrap(),
            };
            if overflow_byte_count != 0 {
                // write zero bytes as placeholder
                buffer.resize(0, 0);
                buffer.resize(overflow_byte_count as usize, 0);
            }
            self.file_map.write().unwrap().insert(largest_id, f);
            self.latest_file_size
                .store(overflow_byte_count, Ordering::SeqCst);
        }
        start_pos
    }

    pub fn prune_head(&self, off: i64) -> Option<Box<dyn error::Error>> {
        let file_id = off / self.block_size;
        let mut file_map = self.file_map.write().unwrap();
        let mut id_list = Vec::with_capacity(file_map.len());
        for (&id, _) in file_map.iter() {
            if id >= file_id {
                continue;
            }
            id_list.push(id)
        }
        for id in id_list {
            file_map.remove(&id);
            let file_name = format!("{}/{}-{}", self.dir_name, id, self.block_size);
            match remove_file(file_name) {
                Ok(_) => {}
                Err(err) => return Some(err.into()),
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct PreReader {
    buffer: Box<[u8; PRE_READ_BUF_SIZE]>,
    file_id: i64,
    start: i64,
    end: i64,
}

impl PreReader {
    pub fn new() -> PreReader {
        PreReader {
            buffer: Box::new([0; PRE_READ_BUF_SIZE]),
            file_id: 0,
            start: 0,
            end: 0,
        }
    }

    fn fill_slice<F>(&mut self, file_id: i64, start: i64, access: F)
    where
        F: Fn(&mut [u8]) -> i64,
    {
        self.file_id = file_id;
        self.start = start;
        let n = access(self.buffer[..].as_mut());
        self.end = start + n;
    }

    fn try_read(&self, file_id: i64, start: i64, bz: &mut [u8]) -> bool {
        if file_id == self.file_id && self.start <= start && start + bz.len() as i64 <= self.end {
            let offset = (start - self.start) as usize;
            bz.copy_from_slice(&self.buffer[offset..offset + bz.len()]);
            return true;
        }
        false
    }
}

#[cfg(test)]
mod hp_file_tests {
    use super::*;
    use crate::test_helper::TempDir;

    #[test]
    fn pre_reader() {
        const FILE_NAME: &str = "hpfile_test_1.txt";
        let _ = remove_file(FILE_NAME);
        let mut f = File::create_new(FILE_NAME).unwrap();
        let buf = [0u8; 8];
        f.write(&buf).unwrap();
        let buf = [1u8; 8];
        f.write(&buf).unwrap();

        let mut r = PreReader {
            buffer: Box::new([0; PRE_READ_BUF_SIZE]),
            file_id: 0,
            start: 0,
            end: 0,
        };
        let file_id = 1;
        let pos = 8;
        r.fill_slice(file_id, pos, |slice: &mut [u8]| -> i64 {
            match f.read_at(slice, pos as u64) {
                Ok(n) => {
                    return n as i64;
                }
                Err(e) => {
                    panic!("{}", e);
                }
            }
        });
        assert_eq!(r.start, 8);
        assert_eq!(r.end, 8 + 8);
        assert_eq!(r.buffer[0], 1);
        assert_eq!(r.buffer[7], 1);
        assert_eq!(r.buffer[8], 0);

        let mut buf = [0; 4];
        let res = r.try_read(file_id, 8, &mut buf);
        assert_eq!(res, true);
        assert_eq!(buf[0], 1);
        assert_eq!(buf[3], 1);
        let _ = remove_file(FILE_NAME);
    }

    #[test]
    fn hp_file_new() {
        let dir = TempDir::new("hpfile_test_dir_2");
        let buffer_size = 64;
        let block_size = 128;
        let mut hp = HPFile::new(buffer_size, block_size, dir.to_string()).unwrap();
        assert_eq!(hp.buffer_size, buffer_size);
        assert_eq!(hp.block_size, block_size);
        assert_eq!(hp.file_map.read().unwrap().len(), 1);
        assert_eq!(
            hp.file_map
                .write()
                .unwrap()
                .get(&0)
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
            0
        );

        let slice0 = [1; 44];
        let mut buffer = vec![];
        let mut pos = hp.append(&slice0.to_vec(), &mut buffer);
        assert_eq!(0, pos);
        assert_eq!(44, hp.size());

        let slice1a = [2; 16];
        let slice1b = [3; 10];
        let mut slice1 = vec![];
        slice1.extend_from_slice(&slice1a);
        slice1.extend_from_slice(&slice1b);
        pos = hp.append(slice1.as_ref(), &mut buffer);
        assert_eq!(44, pos);
        assert_eq!(70, hp.size());

        let slice2a = [4; 25];
        let slice2b = [5; 25];
        let mut slice2 = vec![];
        slice2.extend_from_slice(&slice2a);
        slice2.extend_from_slice(&slice2b);
        pos = hp.append(slice2.as_ref(), &mut buffer);
        assert_eq!(70, pos);
        assert_eq!(120, hp.size());

        let mut check0 = [0; 44];
        hp.read_at(&mut check0, 0);
        assert_eq!(slice0, check0);

        hp.flush(&mut buffer);

        let mut check1 = [0; 26];
        hp.read_at(&mut check1, 44);
        assert_eq!(slice1, check1);

        let mut check2 = [0; 50];
        hp.read_at(&mut check2, 70);
        assert_eq!(slice2, check2);

        let slice3 = [0; 16];
        pos = hp.append(slice3.to_vec().as_ref(), &mut buffer);
        assert_eq!(120, pos);
        assert_eq!(136, hp.size());

        hp.flush(&mut buffer);
        hp.close();

        let mut hp_new;
        match HPFile::new(64, 128, dir.to_string()) {
            Ok(f) => hp_new = f,
            Err(err) => {
                panic!("{}", err)
            }
        }

        hp_new.read_at(&mut check0, 0);
        assert_eq!(slice0, check0);

        hp_new.read_at(&mut check1, 44);
        assert_eq!(slice1, check1);

        hp_new.read_at(&mut check2, 70);
        assert_eq!(slice2, check2);

        let mut check3 = [0; 16];
        hp_new.read_at(&mut check3, 120);
        assert_eq!(slice3, check3);

        match hp_new.prune_head(64) {
            None => {}
            Some(err) => {
                panic!("{}", err)
            }
        }
        match hp_new.truncate(120) {
            None => {}
            Some(err) => {
                panic!("{}", err)
            }
        }
        assert_eq!(hp_new.size(), 120);
        let mut slice4 = vec![];
        hp_new.read_at(&mut slice4, 120);
        assert_eq!(slice4.len(), 0);
    }

    #[test]
    #[should_panic(expected = "Invalid blockSize:127 bufferSize:64")]
    fn test_new_file_invalid_buffer_or_block_size() {
        let dir = TempDir::new("test_new_file_invalid_buffer_or_block_size");
        let buffer_size = 64;
        let block_size = 127;
        let _ = HPFile::new(buffer_size, block_size, dir.to_string()).unwrap();
    }

    #[test]
    fn test_new_file_invalid_filename() {
        let dir = TempDir::new("test_new_file_invalid_filename");
        dir.create_file("hello.txt"); // invalid filename
        assert_eq!(
            "hello.txt does not match the pattern 'FileId-BlockSize'",
            HPFile::new(64, 128, dir.to_string())
                .unwrap_err()
                .to_string()
        )
    }

    #[test]
    fn test_new_file_invalid_filename2() {
        let dir = TempDir::new("test_new_file_invalid_filename2");
        dir.create_file("hello-hello.txt"); // invalid filename
        assert_eq!(
            "invalid digit found in string",
            HPFile::new(64, 128, dir.to_string())
                .unwrap_err()
                .to_string()
        )
    }

    #[test]
    fn test_new_file_invalid_filename3() {
        let dir = TempDir::new("test_new_file_invalid_filename3");
        dir.create_file("1-hello.txt"); // invalid xx-xx filename
        assert_eq!(
            "Invalid Filename: 1-hello.txt",
            HPFile::new(64, 128, dir.to_string())
                .unwrap_err()
                .to_string()
        )
    }

    #[test]
    fn test_new_file_failed_invalid_size() {
        let dir = TempDir::new("test_new_file_failed_invalid_size");
        dir.create_file("1-1"); // invalid file size not equal block size
        assert_eq!(
            "Invalid Size! 1!=128",
            HPFile::new(64, 128, dir.to_string())
                .unwrap_err()
                .to_string()
        )
    }

    #[test]
    fn test_read_at_with_pre_reader() {
        let dir = TempDir::new("hpfile_test_dir_4");
        let buffer_size = 64;
        let block_size = 128;
        let hp_file = HPFile::new(buffer_size, block_size, dir.to_string()).unwrap();
        let mut pre_reader = PreReader::new();
        pre_reader.end = 5;
        for i in 0..5 {
            pre_reader.buffer[i as usize] = i;
        }
        let mut buf = Vec::from([0; 10]);
        hp_file.read_at_with_pre_reader(&mut buf, 3, 0, &mut pre_reader);
        assert_eq!(buf[0..3], [0, 1, 2]);
        let mut buf = Vec::from([0; 129]);
        fs::write("./hpfile_test_dir_4/0-128", [1, 2, 3, 4]).unwrap();
        hp_file.read_at_with_pre_reader(&mut buf, 129, 0, &mut pre_reader);
        assert_eq!(buf[0..4], [1, 2, 3, 4]);

        fs::write("./hpfile_test_dir_4/0-128", [1, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap();
        let mut pre_reader = PreReader::new();
        let mut buf = Vec::from([0; 10]);
        hp_file.read_at_with_pre_reader(&mut buf, 9, 0, &mut pre_reader);
        assert_eq!(buf[0..9], [1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(pre_reader.end, 9);
    }

    #[test]
    fn test_prune_head() {
        let dir = TempDir::new("hpfile_test_dir_5");
        let buffer_size = 64;
        let block_size = 128;
        let hp_file = HPFile::new(buffer_size, block_size, dir.to_string()).unwrap();
        hp_file.prune_head(block_size * 2);
        assert_eq!(fs::read_dir(dir.to_string()).unwrap().count(), 0);
    }

    #[test]
    fn test_hpfile() {
        let dir = TempDir::new("hpfile_test_dir_6");
        let buffer_size = 64;
        let block_size = 128;
        let hp_file = HPFile::new(buffer_size, block_size, dir.to_string()).unwrap();
        let mut buffer = Vec::with_capacity(buffer_size as usize);

        for _i in 0..100 {
            hp_file.append("aaaaaaaaaaaaaaaaaaaa".as_bytes(), &mut buffer);
            hp_file.flush(&mut buffer);
        }

        assert_eq!(
            dir.list().join(","),
            [
                "hpfile_test_dir_6/0-128",
                "hpfile_test_dir_6/1-128",
                "hpfile_test_dir_6/10-128",
                "hpfile_test_dir_6/11-128",
                "hpfile_test_dir_6/12-128",
                "hpfile_test_dir_6/13-128",
                "hpfile_test_dir_6/14-128",
                "hpfile_test_dir_6/15-128",
                "hpfile_test_dir_6/2-128",
                "hpfile_test_dir_6/3-128",
                "hpfile_test_dir_6/4-128",
                "hpfile_test_dir_6/5-128",
                "hpfile_test_dir_6/6-128",
                "hpfile_test_dir_6/7-128",
                "hpfile_test_dir_6/8-128",
                "hpfile_test_dir_6/9-128",
            ]
            .join(",")
        );

        hp_file.prune_head(500);

        assert_eq!(
            dir.list().join(","),
            [
                "hpfile_test_dir_6/10-128",
                "hpfile_test_dir_6/11-128",
                "hpfile_test_dir_6/12-128",
                "hpfile_test_dir_6/13-128",
                "hpfile_test_dir_6/14-128",
                "hpfile_test_dir_6/15-128",
                "hpfile_test_dir_6/3-128",
                "hpfile_test_dir_6/4-128",
                "hpfile_test_dir_6/5-128",
                "hpfile_test_dir_6/6-128",
                "hpfile_test_dir_6/7-128",
                "hpfile_test_dir_6/8-128",
                "hpfile_test_dir_6/9-128",
            ]
            .join(",")
        );
    }
}
