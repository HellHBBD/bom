#![cfg_attr(not(target_os = "windows"), allow(dead_code))]

use semver::Version;

pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const OWNER: &str = "HellHBBD";
const REPOSITORY: &str = "bom";
const ASSET_SUFFIX: &str = "-windows-x86_64-setup.exe";

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateInfo {
    pub version: Version,
    pub tag: String,
    pub asset_name: String,
    pub download_url: String,
    pub expected_size: u64,
    pub expected_sha256: String,
    pub release_notes: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UpdateError {
    InvalidVersion,
    MissingInstaller,
    MissingDigest,
    InvalidDigest,
    Network,
    NoRelease,
    Download,
    IncompleteDownload,
    DigestMismatch,
    InstallerLaunch,
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidVersion => "發布版本格式不正確",
            Self::MissingInstaller => "此版本沒有可用的 Windows x86_64 安裝程式",
            Self::MissingDigest => "此版本的安裝程式缺少完整性資訊",
            Self::InvalidDigest => "此版本的安裝程式完整性資訊無效",
            Self::Network => "無法連線至 GitHub，請確認網路連線後再試一次",
            Self::NoRelease => "目前尚未發布正式版本",
            Self::Download => "無法下載更新檔案，請稍後再試一次",
            Self::IncompleteDownload => "更新檔案下載不完整",
            Self::DigestMismatch => "更新檔案完整性驗證失敗",
            Self::InstallerLaunch => "無法啟動安裝程式",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for UpdateError {}

pub fn parse_release_version(tag: &str) -> Result<Version, UpdateError> {
    let version = tag
        .strip_prefix('v')
        .ok_or(UpdateError::InvalidVersion)
        .and_then(|value| Version::parse(value).map_err(|_| UpdateError::InvalidVersion))?;
    if version.pre.is_empty() {
        Ok(version)
    } else {
        Err(UpdateError::InvalidVersion)
    }
}

pub fn installer_asset_name(version: &Version) -> String {
    format!("BOM-v{version}{ASSET_SUFFIX}")
}

pub fn parse_sha256_digest(digest: Option<&str>) -> Result<String, UpdateError> {
    let hash = digest
        .ok_or(UpdateError::MissingDigest)?
        .strip_prefix("sha256:")
        .ok_or(UpdateError::InvalidDigest)?;
    if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(hash.to_ascii_lowercase())
    } else {
        Err(UpdateError::InvalidDigest)
    }
}

#[cfg(target_os = "windows")]
mod windows {
    use std::path::PathBuf;
    use std::process::Command;
    use std::time::Duration;

    use reqwest::{Client, StatusCode};
    use serde::Deserialize;
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncWriteExt;

    use super::{
        installer_asset_name, parse_release_version, parse_sha256_digest, UpdateError, UpdateInfo,
        APP_VERSION, OWNER, REPOSITORY,
    };

    const GITHUB_API_VERSION: &str = "2026-03-10";

    #[derive(Deserialize)]
    struct GithubRelease {
        tag_name: String,
        body: Option<String>,
        assets: Vec<GithubAsset>,
    }

    #[derive(Deserialize)]
    struct GithubAsset {
        name: String,
        browser_download_url: String,
        size: u64,
        digest: Option<String>,
    }

    fn client() -> Result<Client, UpdateError> {
        Client::builder()
            .user_agent(format!("BOM-Updater/{APP_VERSION}"))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|error| {
                eprintln!("無法建立更新 HTTP client：{error}");
                UpdateError::Network
            })
    }

    pub async fn check_for_update() -> Result<Option<UpdateInfo>, UpdateError> {
        let response = client()?
            .get(format!(
                "https://api.github.com/repos/{OWNER}/{REPOSITORY}/releases/latest"
            ))
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .send()
            .await
            .map_err(|error| {
                eprintln!("檢查更新失敗：{error}");
                UpdateError::Network
            })?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(UpdateError::NoRelease);
        }
        let response = response.error_for_status().map_err(|error| {
            eprintln!("GitHub Release API 回應失敗：{error}");
            UpdateError::Network
        })?;
        let release: GithubRelease = response.json().await.map_err(|error| {
            eprintln!("GitHub Release API 資料格式錯誤：{error}");
            UpdateError::Network
        })?;
        let version = parse_release_version(&release.tag_name)?;
        let current = Version::parse(APP_VERSION).map_err(|_| UpdateError::InvalidVersion)?;
        if version <= current {
            return Ok(None);
        }

        let asset_name = installer_asset_name(&version);
        let asset = release
            .assets
            .into_iter()
            .find(|asset| asset.name == asset_name)
            .ok_or(UpdateError::MissingInstaller)?;
        if asset.size == 0 {
            return Err(UpdateError::MissingInstaller);
        }

        Ok(Some(UpdateInfo {
            version,
            tag: release.tag_name,
            asset_name: asset.name,
            download_url: asset.browser_download_url,
            expected_size: asset.size,
            expected_sha256: parse_sha256_digest(asset.digest.as_deref())?,
            release_notes: release.body.filter(|body| !body.trim().is_empty()),
        }))
    }

    pub async fn download_update(update: &UpdateInfo) -> Result<PathBuf, UpdateError> {
        let updates_dir = std::env::temp_dir().join("BOM").join("updates");
        if updates_dir.exists() {
            tokio::fs::remove_dir_all(&updates_dir)
                .await
                .map_err(|error| {
                    eprintln!("無法清理舊更新檔案：{error}");
                    UpdateError::Download
                })?;
        }
        tokio::fs::create_dir_all(&updates_dir)
            .await
            .map_err(|error| {
                eprintln!("無法建立更新暫存目錄：{error}");
                UpdateError::Download
            })?;

        let response = client()?
            .get(&update.download_url)
            .send()
            .await
            .map_err(|error| {
                eprintln!("下載更新失敗：{error}");
                UpdateError::Download
            })?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(UpdateError::MissingInstaller);
        }
        let mut response = response.error_for_status().map_err(|error| {
            eprintln!("更新下載回應失敗：{error}");
            UpdateError::Download
        })?;

        let installer = updates_dir.join(&update.asset_name);
        let part = updates_dir.join(format!("{}.part", update.asset_name));
        let mut file = tokio::fs::File::create(&part).await.map_err(|error| {
            eprintln!("無法建立更新暫存檔：{error}");
            UpdateError::Download
        })?;
        let mut downloaded = 0_u64;
        let mut hasher = Sha256::new();
        let result = async {
            while let Some(chunk) = response.chunk().await.map_err(|error| {
                eprintln!("讀取更新下載內容失敗：{error}");
                UpdateError::Download
            })? {
                downloaded += chunk.len() as u64;
                hasher.update(&chunk);
                file.write_all(&chunk).await.map_err(|error| {
                    eprintln!("寫入更新檔案失敗：{error}");
                    UpdateError::Download
                })?;
            }
            file.flush().await.map_err(|_| UpdateError::Download)?;
            file.sync_all().await.map_err(|_| UpdateError::Download)?;
            if downloaded != update.expected_size {
                return Err(UpdateError::IncompleteDownload);
            }
            let actual = format!("{:x}", hasher.finalize());
            if actual != update.expected_sha256 {
                return Err(UpdateError::DigestMismatch);
            }
            Ok(())
        }
        .await;
        drop(file);
        if let Err(error) = result {
            let _ = tokio::fs::remove_file(&part).await;
            return Err(error);
        }
        tokio::fs::rename(&part, &installer)
            .await
            .map_err(|error| {
                eprintln!("無法完成更新檔案：{error}");
                UpdateError::Download
            })?;
        Ok(installer)
    }

    pub fn launch_installer(installer: &std::path::Path) -> Result<(), UpdateError> {
        if installer
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("exe")
        {
            return Err(UpdateError::InstallerLaunch);
        }
        Command::new(installer).spawn().map_err(|error| {
            eprintln!("無法啟動更新安裝程式：{error}");
            UpdateError::InstallerLaunch
        })?;
        Ok(())
    }
}

#[cfg(target_os = "windows")]
pub use windows::{check_for_update, download_update, launch_installer};

#[cfg(test)]
mod tests {
    use super::{installer_asset_name, parse_release_version, parse_sha256_digest, UpdateError};

    #[test]
    fn parses_stable_release_versions() {
        assert_eq!(
            parse_release_version("v0.1.1").unwrap().to_string(),
            "0.1.1"
        );
        assert!(
            parse_release_version("v0.2.0").unwrap() > parse_release_version("v0.1.99").unwrap()
        );
        assert!(matches!(
            parse_release_version("0.1.1"),
            Err(UpdateError::InvalidVersion)
        ));
        assert!(matches!(
            parse_release_version("v0.1.1-beta.1"),
            Err(UpdateError::InvalidVersion)
        ));
    }

    #[test]
    fn builds_exact_windows_setup_asset_name() {
        let version = parse_release_version("v0.1.1").unwrap();
        assert_eq!(
            installer_asset_name(&version),
            "BOM-v0.1.1-windows-x86_64-setup.exe"
        );
    }

    #[test]
    fn accepts_only_a_valid_sha256_digest() {
        let hash = "a".repeat(64);
        assert_eq!(
            parse_sha256_digest(Some(&format!("sha256:{hash}"))).unwrap(),
            hash
        );
        assert!(matches!(
            parse_sha256_digest(Some("sha512:abc")),
            Err(UpdateError::InvalidDigest)
        ));
    }
}
