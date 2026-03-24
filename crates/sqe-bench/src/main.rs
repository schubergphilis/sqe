mod cli;
mod generate;
// mod load;
// mod client;
// mod test;
// mod compare;
// mod report;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = cli::Cli::parse();
    match cli.command {
        cli::Command::Generate {
            benchmark,
            scale,
            output,
            ..
        } => {
            let gen = generate::get_generator(&benchmark)?;
            for table_def in gen.tables() {
                println!("Generating {}.{}...", benchmark, table_def.name);
                let stats = gen.generate_table(&table_def.name, scale, &output)?;
                println!(
                    "  {} rows, {} files, {:.1}s",
                    stats.rows,
                    stats.files,
                    stats.duration.as_secs_f64()
                );
            }
            println!("Done.");
            Ok(())
        }
        cli::Command::Load { .. } => todo!("load command not yet implemented"),
        cli::Command::Test { .. } => todo!("test command not yet implemented"),
    }
}
