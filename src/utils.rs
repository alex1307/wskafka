use crate::INIT_LOGGER;
use log::{error, info};
use std::{
    fmt,
    io::{Read, Write},
};

pub fn configure_log4rs(file: &str) {
    INIT_LOGGER.call_once(|| {
        log4rs::init_file(file, Default::default()).unwrap();
        info!("SUCCESS: Loggers are configured with dir: _log/*");
    });
}
