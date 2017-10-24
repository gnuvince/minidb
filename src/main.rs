#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate bincode;
extern crate env_logger;

use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, OpenOptions, create_dir_all, remove_dir_all};
use std::path::PathBuf;
use std::io::{BufReader, BufWriter};


const REPLAY_LOG: &'static str = "replay.log";
const DB_SNAPSHOT: &'static str = "db.snapshot";


type Result<T> = std::result::Result<T, Box<Error>>;


#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum Typing {
    Dynamic,
    Static
}

// The data that we want to store in the database.
#[derive(Debug, Serialize, Deserialize)]
struct LanguageInfo {
    creator: String,
    year: u16,
    typing: Typing
}

#[derive(Debug, Serialize, Deserialize)]
struct Db {
    dir: PathBuf,
    replay_log: PathBuf,
    db_snapshot: PathBuf,
    data: HashMap<String, LanguageInfo>,
    enable_logging: bool
}

impl Db {
    fn new(dir: PathBuf) -> Self {
        let replay_log = dir.join(REPLAY_LOG);
        let db_snapshot = dir.join(DB_SNAPSHOT);
        Db {
            dir,
            replay_log,
            db_snapshot,
            data: HashMap::with_capacity(1024),
            enable_logging: true
        }
    }


    /// Initializes a new database; if `dir` looks like a database
    /// directory, load from disk, otherwise create an empty database.
    pub fn load_or_new(dir: PathBuf) -> Result<Self> {
        if Self::is_db_dir(&dir) {
            Self::restore_and_replay(dir)
        }
        else {
            Self::create(dir)
        }
    }


    /// Ensures that the database directory and related files exist.
    fn is_db_dir(dir: &PathBuf) -> bool {
        // If there isn't a snapshot file, we replay the log
        // against an empty database.
        dir.is_dir() && dir.join(REPLAY_LOG).is_file()
    }

    /// Loads the database from the snapshot + replay log
    fn restore_and_replay(dir: PathBuf) -> Result<Self> {
        debug!("Restoring database from snapshot");
        let db_snapshot = dir.join(DB_SNAPSHOT);
        let mut db: Db =
            if db_snapshot.is_file() {
                let fd = File::open(db_snapshot)?;
                let mut buf_reader = BufReader::new(fd);
                bincode::deserialize_from(&mut buf_reader, bincode::Infinite)?
            } else {
                Self::new(dir)
            };
        db.replay()?;
        return Ok(db);
    }


    /// Creates a new, empty database.
    fn create(dir: PathBuf) -> Result<Self> {
        debug!("Creating new database");
        create_dir_all(&dir)?;
        return Ok(Self::new(dir));
    }

    /// Adds a new key/value pair to the database; the pair
    /// is stored in memory and in the replay log on disk.
    pub fn add(&mut self, key: String, value: LanguageInfo) {
        // XXX(vfoley): in real version, the replay log should remain opened.
        if self.enable_logging {
            let fd = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.replay_log)
                .unwrap();
            let mut buf_writer = BufWriter::new(fd);
            let pair = (&key, &value);

            let ser_result = bincode::serialize_into(
                &mut buf_writer, &pair, bincode::Infinite);
            match ser_result {
                Ok(()) => (),
                Err(e) => { error!("{}", e); }
            }
        }
        self.data.insert(key, value);
    }

    #[allow(dead_code)]
    pub fn get(&self, key: &str) -> Option<&LanguageInfo> {
        self.data.get(key)
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn save(&self) {
        let fd = File::create(&self.db_snapshot).unwrap();
        let mut buf_writer = BufWriter::new(fd);
        let _ = bincode::serialize_into(&mut buf_writer, &self, bincode::Infinite);
        let _ = File::create(&self.replay_log); // empty replay log
    }

    pub fn replay(&mut self) -> Result<()> {
        let prev_logging = self.enable_logging;
        self.enable_logging = false;
        let fd = File::open(&self.replay_log)?;
        let mut buf_reader = BufReader::new(fd);
        loop {
            let res: bincode::Result<(String, LanguageInfo)> =
                bincode::deserialize_from(&mut buf_reader, bincode::Infinite);
            match res {
                Ok((name, person)) => { self.add(name, person); }
                Err(_) => { break; }
            }
        }
        self.enable_logging = prev_logging;
        return Ok(());
    }
}

fn main() {
    env_logger::init().unwrap();

    const DB_DIR: &'static str = "/tmp/minidb";

    let _ = remove_dir_all(DB_DIR);

    {
        let mut db = Db::load_or_new(PathBuf::from(DB_DIR)).unwrap();

        db.add("C".to_string(), LanguageInfo {
            creator: "Dennis Ritchie".to_string(),
            year: 1972,
            typing: Typing::Static
        });
        db.add("Python".to_string(), LanguageInfo {
            creator: "Guido van Rossum".to_string(),
            year: 1989,
            typing: Typing::Dynamic
        });

        println!("DB after initial insertions: {:#?}", db);
    }

    {
        let db = Db::load_or_new(PathBuf::from(DB_DIR)).unwrap();
        println!("DB loaded from log only: {:#?}", db);
    }

    {
        let db = Db::load_or_new(PathBuf::from(DB_DIR)).unwrap();
        db.save();
    }

    {
        let mut db = Db::load_or_new(PathBuf::from(DB_DIR)).unwrap();
        println!("DB loaded from snapshot only: {:#?}", db);

        db.add("Go".to_string(), LanguageInfo {
            creator: "Rob Pike".to_string(),
            year: 2009,
            typing: Typing::Static
        });
    }

    {
        let db = Db::load_or_new(PathBuf::from(DB_DIR)).unwrap();
        println!("DB loaded from snapshot + replay: {:#?}", db);
    }
}
