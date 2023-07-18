use flexi_logger::DeferredNow;
use flexi_logger::filter::{LogLineFilter, LogLineWriter};
use log::Record;

pub struct ModuleLogFilter {
    pub disable_modules: Vec<&'static str>,
}

impl LogLineFilter for ModuleLogFilter {
    fn write(&self, now: &mut DeferredNow, record: &Record, log_line_writer: &dyn LogLineWriter) -> std::io::Result<()> {
        if !self.disable_modules.iter().any(|s| {
            record.target().starts_with(*s)
        }) {
            log_line_writer.write(now, record)
        } else {
            Ok(())
        }
    }
}

pub fn with_line(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    write!(
        w,
        "[{}] {} [{:?}] [{}:{}] {}",
        now.now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
        record.level(),
        std::thread::current().id(),
        record.file().unwrap_or("<unnamed>"),
        record.line().unwrap_or(0),
        &record.args()
    )
}