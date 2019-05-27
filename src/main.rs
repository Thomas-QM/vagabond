extern crate clap;
extern crate kankyo;
extern crate yansi;
extern crate r2d2;
#[macro_use] extern crate cdrs;

use clap::*;

use std::fs;
use std::env::var;
use std::path::Path;

use yansi::{Paint};

use cdrs::authenticators::*;
use cdrs::cluster::{ClusterTcpConfig, TcpConnectionsManager, NodeTcpConfigBuilder, session::{self, Session}};
use cdrs::load_balancing::SingleNode;
use cdrs::query::*;
use cdrs::types::IntoRustByIndex;

fn iored(s: &str) {
    println!("{}", Paint::red(s));
}

fn iook(s: &str) {
    println!("{}", Paint::green(s));
}

fn ioprint(s: &str) {
    println!("{}", Paint::yellow(s));
}

pub trait FancyResult<T> {
    fn ioexpect(self, s: &str) -> T;
    fn iook(self, s: &str) -> Self;
}

impl<T, E: std::fmt::Display> FancyResult<T> for std::result::Result<T, E> {
    fn ioexpect(self, s: &str) -> T {
        match self {
            Ok(x) => x,
            Err(e) => {
                iored(s);
                ioprint(&format!("{}", e));
                std::process::exit(-1);
            }
        }
    }

    fn iook(self, s: &str) -> Self {
        match &self {
            Ok(_) => iook(s),
            _ => ()
        }

        self
    }
}

impl<T> FancyResult<T> for Option<T> {
    fn ioexpect(self, s: &str) -> T {
        match self {
            Some(x) => x,
            None => {
                iored(s);
                std::process::exit(-1);
            }
        }
    }

    fn iook(self, s: &str) -> Self {
        match &self {
            Some(_) => iook(s),
            _ => ()
        }

        self
    }
}

pub struct Config {
    vagabond: String,

    migrations: Vec<String>,
    host: String,
    username: Option<String>,
    password: Option<String>,
    keyspace: Option<String>
}

fn get_cfg() -> Config {
    let mut migrations: Vec<String> = Vec::new();

    let s = fs::read_to_string("./migrations/vagabond").ioexpect("Error reading vagabond. Make sure the directory is intialized.");
    for line in s.split('\n') {
        if !line.starts_with("//") {
            let lines = line.to_owned();
            if migrations.contains(&lines) {
                panic!("Migration name {} is already used!", &lines);
            }

            migrations.push(lines);
        }
    }

    Config {
        vagabond: s,
        migrations,
        host: var("CASSANDRA_HOST").ioexpect("Required CASSANDRA_HOST environment variable not found"),
        username: var("CASSANDRA_USER").ok(),
        password: var("CASSANDRA_PASSWORD").ok(),
        keyspace: var("CASSANDRA_KEYSPACE").ok()
    }
}

#[derive(Clone)]
pub enum PasswordOrNoneAuth {
    Password(StaticPasswordAuthenticator),
    NoAuth(NoneAuthenticator)
}

impl Authenticator for PasswordOrNoneAuth {
    fn get_auth_token(&self) -> cdrs::types::CBytes {
        match self {
            PasswordOrNoneAuth::Password(x) => x.get_auth_token(),
            PasswordOrNoneAuth::NoAuth(x) => x.get_auth_token()
        }
    }

    fn get_cassandra_name(&self) -> Option<&str> {
        match self {
            PasswordOrNoneAuth::Password(x) => x.get_cassandra_name(),
            PasswordOrNoneAuth::NoAuth(x) => x.get_cassandra_name()
        }
    }
}

pub type VBSession<'a> = Session<SingleNode<r2d2::Pool<TcpConnectionsManager<PasswordOrNoneAuth>>>>;

fn init_single_connection(cfg: &Config) -> VBSession {
    let auth = match (&cfg.username, &cfg.password) {
        (Some(user), Some(pass)) =>
            PasswordOrNoneAuth::Password(StaticPasswordAuthenticator::new(user.clone(), pass.clone())),
        (None, None) => PasswordOrNoneAuth::NoAuth(NoneAuthenticator),
        _ => {
            ioprint("One of username and password have been provided, but not both. Continuing with no authentication.");
            
            PasswordOrNoneAuth::NoAuth(NoneAuthenticator {})
        }
    };

    let ses = session::new(&ClusterTcpConfig(vec![NodeTcpConfigBuilder::new(&cfg.host, auth).build()]), SingleNode::new())
        .ioexpect("Error initializing session");

    if let Some(x) = &cfg.keyspace {
        //injection but meh, apparently you cant bind variables to USE
        ses.query(format!("USE {}", x)).ioexpect("Error setting keyspace. Does it exist?");
    } else {
        ioprint("No keyspace specified. The next operation may or may not error.")
    }

    ses.query("CREATE TABLE IF NOT EXISTS vagabond (migration TEXT, PRIMARY KEY(migration));").ioexpect("Error creating vagabond table");
    ses
}

fn apply_migration(session: &VBSession, migration: String) {
    for query in migration.split(";") {
        if query.len() == 0 {
            break;
        }

        session.query(query).ioexpect(&format!("Error applying migration query: {}. You should probably clean this up", query));
    }
}

fn get_current_migration(session: &VBSession) -> Option<String> {
    let body = session.query("SELECT migration FROM vagabond").unwrap().get_body().unwrap();
    if let Some(rows) = body.into_rows() {
        return rows.first().map(|x| {
            x.get_r_by_index(0).unwrap()
        });
    }

    None
}

fn del_current_migration(session: &VBSession) {
    session.query("TRUNCATE vagabond").unwrap();
}

fn set_current_migration(session: &VBSession, name: &str) {
    del_current_migration(session);
    session.query_with_values("INSERT INTO vagabond (migration) VALUES (?)", query_values!(name)).ioexpect("Error setting migration in database");
}

fn main() {
    kankyo::load().unwrap();

    let matches = clap_app!(vagabond =>
        (version: crate_version!())
        (author: crate_authors!())
        (about: crate_description!())
        (about: "A very simple cassandra migration tool for rust.")
        
        (@subcommand init =>
            (about: "Initialize the migrations directory")
        )

        (@subcommand new =>
            (about: "Add new migration")
            (@arg NAME: * "Name of migration")
        )

        (@subcommand redo => 
            (about: "Undoes and applies the last migration")
        )

        (@subcommand rollback => 
            (about: "Undoes the last migration")
        )

        (@subcommand apply => 
            (about: "Applies the next migration")
        )

        (@subcommand delete => 
            (about: "Deletes all unapplied migrations")
        )
    ).get_matches();
    
    match matches.subcommand() {
        ("init", _) => {
            fs::create_dir("./migrations").ioexpect("Cannot create directory");
            fs::write("./migrations/vagabond", "//list of migration names in order, current migration is stored in db.").ioexpect("Error writing file");
            iook("./migrations initialized");
        },
        ("new", Some(args)) => {
            let name: &str = args.value_of("NAME").unwrap();
            let cfg = get_cfg();

            if cfg.migrations.contains(&name.to_owned()) {
                panic!("Migration name is already used!");
            }

            let mut path = Path::new("./migrations/").to_path_buf();
            path.push(name);

            fs::create_dir(&path).ioexpect("Cannot create directory");

            path.push("./up.cql");
            fs::write(&path, "").ioexpect("Error creating up.cql");
            
            fs::write(path.with_file_name("down.cql"), "").ioexpect("Error creating down.cql");

            fs::write("./migrations/vagabond", format!("{}\n{}", cfg.vagabond, name)).ioexpect("Error writing to vagabond");

            iook("Migration created");
        },
        ("redo", _) => {
            let cfg = get_cfg();
            let ses = init_single_connection(&cfg);

            let mut path = Path::new("./migrations").to_path_buf();
            path.push(&get_current_migration(&ses).ioexpect("No migration currently applied"));

            path.push("down.cql");
            ioprint("Applying down.cql");
            apply_migration(&ses, fs::read_to_string(&path).ioexpect("Error reading down.cql"));
            ioprint("Applying up.cql");
            apply_migration(&ses, fs::read_to_string(path.with_file_name("up.cql")).ioexpect("Error reading up.cql"));

            iook("Redone successfully");
        },
        ("rollback", _) => {
            let cfg = get_cfg();
            let ses = init_single_connection(&cfg);
            let cur = get_current_migration(&ses).ioexpect("No migration currently applied");

            let mut path = Path::new("./migrations").to_path_buf();
            path.push(&cur);

            path.push("down.cql");
            apply_migration(&ses, fs::read_to_string(path).ioexpect("Error reading down.cql"));

            for (i, x) in cfg.migrations.iter().enumerate() {
                if x.as_str() == cur {
                    if i > 0 {
                        set_current_migration(&ses, cfg.migrations[i-1].as_str());
                    } else {
                        del_current_migration(&ses);
                    }
                }
            }

            iook("Rolled back")
        },
        ("apply", _) => {
            let cfg = get_cfg();
            let ses = init_single_connection(&cfg);
            
            let name = match get_current_migration(&ses) {
                None => cfg.migrations.first(),
                Some(cur) => {
                    let mut applied = false;
                    let mut next = None;
                    
                    for x in &cfg.migrations {
                        if applied {
                            next = Some(x);
                            
                            break;
                        } else if x.as_str() == cur {
                            applied = true;
                        }
                    }

                    next
                }
            }.ioexpect("No migration to apply!");

            let mut path = Path::new("./migrations").to_path_buf();
            path.push(name);
            path.push("up.cql");

            apply_migration(&ses, fs::read_to_string(path).ioexpect("Error reading up.cql"));
            set_current_migration(&ses, name);

            iook(&format!("Applied {}", name));
        },
        ("delete", _) => {
            let cfg = get_cfg();
            let ses = init_single_connection(&cfg);

            let mut path = Path::new("./migrations").to_path_buf();
            path.push("migration");

            let mut vagabond: String = format!("\n{}\n", &cfg.vagabond);

            {
                let mut remove = |x| {
                    ioprint(&format!("Deleting {}", x));
                    fs::remove_dir_all(path.with_file_name(&x)).ioexpect("Error deleting directory");
                    vagabond = vagabond.replace(&format!("\n{}\n", x), "\n");
                };
                
                match get_current_migration(&ses) {
                    Some(cur) => {
                        let mut applied = true;

                        for x in cfg.migrations {
                            if applied {
                                if x == cur {
                                    applied = false;
                                }
                            } else {
                                remove(x);
                            }
                        }
                    },
                    None => {
                        for x in cfg.migrations {
                            remove(x);
                        }
                    }
                }
            }

            fs::write("./migrations/vagabond", &vagabond[1..vagabond.len()-1]).ioexpect("Error writing to vagabond");
        },
        _ => {
            let cfg = get_cfg();
            let current = get_current_migration(&init_single_connection(&cfg));

            match current {
                Some(cur) => {
                    let mut applied = true;
                    
                    for x in cfg.migrations {
                        if applied {
                            iook(&format!("✅ {}", x));
                            
                            if x == cur {
                                applied = false;
                            }
                        } else {
                            iored(&format!("{}", x));
                        }
                    }
                },
                None => {
                    for x in cfg.migrations {
                        iored(&format!("{}", x));
                    }
                }
            }
        }
    }
}
