//! `wikidata-refs extract`: bundles in, staging Parquet out.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rayon::prelude::*;
use wikidata_refs::fold::{Counts, PageFold};
use wikidata_refs::sink::ParquetSink;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Folds each entity's revisions into reference-added and reference-removed events.
    ///
    /// Reads `.mwrev.zst` bundles that `RevisionChest build` wrote from a history dump
    /// (a page's revisions together, in order) and writes, per bundle,
    /// `<output>/wikidata_reference_events/<bundle>.parquet` and
    /// `<output>/wikidata_reference_snaks/<bundle>.parquet`. A bundle's files are
    /// replaced whole on a re-run. Prints the counts as JSON.
    Extract {
        /// Bundle files.
        bundles: Vec<PathBuf>,
        /// A directory of bundles, every `*.mwrev.zst` directly in it.
        #[arg(short = 'd', long)]
        dir: Option<PathBuf>,
        /// Where the staging datasets go.
        #[arg(short, long)]
        output: PathBuf,
        /// The entity namespaces: Wikidata's items (0) and properties (120).
        #[arg(long, value_delimiter = ',', default_value = "0,120")]
        namespace: Vec<i64>,
        /// Bundles read at once (default: one per core). A bundle needs up to 128 MiB
        /// to decompress, besides the entity being read.
        #[arg(short, long)]
        jobs: Option<usize>,
    },
}

fn stem(bundle: &Path) -> String {
    let name = bundle
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    name.strip_suffix(".mwrev.zst")
        .or_else(|| name.strip_suffix(".zst"))
        .unwrap_or(&name)
        .to_string()
}

fn extract_one(bundle: &Path, output: &Path, namespaces: &[i64]) -> Result<Counts, String> {
    let reader = mwrev::BundleReader::open(bundle).map_err(|e| e.to_string())?;
    let mut sink = ParquetSink::create(output, &stem(bundle)).map_err(|e| e.to_string())?;
    let mut fold = PageFold::new(Some(namespaces.to_vec()));
    for revision in reader {
        let (header, text) = revision.map_err(|e| e.to_string())?;
        fold.revision(&header, &text, &mut sink)
            .map_err(|e| e.to_string())?;
    }
    sink.finish().map_err(|e| e.to_string())?;
    Ok(fold.finish())
}

fn counts_json(c: &Counts, bundles: usize) -> serde_json::Value {
    serde_json::json!({
        "bundles": bundles,
        "revisions": {
            "entities": c.entities,
            "redirects": c.redirects,
            "other_namespace": c.other_namespace,
            "hidden": c.hidden,
            "empty": c.empty,
            "legacy": c.legacy,
            "unparsed": c.unparsed,
        },
        "references": {"added": c.added, "of_which_baseline": c.baseline, "removed": c.removed, "snak_rows": c.snaks},
        "unparsed_samples": c.unparsed_samples.iter()
            .map(|(rev, why)| serde_json::json!({"revision_id": rev, "error": why}))
            .collect::<Vec<_>>(),
    })
}

fn main() -> ExitCode {
    let Cli {
        command:
            Command::Extract {
                mut bundles,
                dir,
                output,
                namespace,
                jobs,
            },
    } = Cli::parse();
    if let Some(dir) = dir {
        match std::fs::read_dir(&dir) {
            Ok(entries) => bundles.extend(
                entries
                    .filter_map(Result::ok)
                    .map(|e| e.path())
                    .filter(|p| p.to_string_lossy().ends_with(".mwrev.zst")),
            ),
            Err(e) => {
                eprintln!("{}: {e}", dir.display());
                return ExitCode::FAILURE;
            }
        }
    }
    bundles.sort();
    bundles.dedup();
    if bundles.is_empty() {
        eprintln!("no bundles given");
        return ExitCode::FAILURE;
    }
    if let Some(jobs) = jobs
        && let Err(e) = rayon::ThreadPoolBuilder::new()
            .num_threads(jobs.max(1))
            .build_global()
    {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    let results: Vec<(PathBuf, Result<Counts, String>)> = bundles
        .par_iter()
        .map(|b| (b.clone(), extract_one(b, &output, &namespace)))
        .collect();
    let mut total = Counts::default();
    let mut failed = false;
    for (bundle, result) in results {
        match result {
            Ok(counts) => total.merge(counts),
            Err(e) => {
                failed = true;
                eprintln!("{}: {e}", bundle.display());
            }
        }
    }
    println!("{:#}", counts_json(&total, bundles.len()));
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
