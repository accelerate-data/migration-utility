use tauri::State;

use crate::db::DbState;
use crate::types::{CommandError, DeviceFlowResponse, GitHubAuthResult, GitHubRepo, GitHubUser};

/// Return the GitHub OAuth App client ID, allowing an env-var override for
/// testing or alternative deployments.
fn github_client_id() -> String {
    std::env::var("GITHUB_CLIENT_ID").unwrap_or_else(|_| "Ov23lioPbQz4gAFxEfhM".to_string())
}

/// Build a `reqwest::Client` pre-configured with GitHub API authentication
/// headers (Bearer token, Accept, User-Agent, API version).
fn github_authenticated_client(token: &str) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "Authorization",
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    headers.insert(
        "Accept",
        reqwest::header::HeaderValue::from_static("application/vnd.github+json"),
    );
    headers.insert(
        "User-Agent",
        reqwest::header::HeaderValue::from_static("MigrationUtility"),
    );
    headers.insert(
        "X-GitHub-Api-Version",
        reqwest::header::HeaderValue::from_static("2022-11-28"),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}

/// Start the GitHub Device Flow by requesting a device code.
#[tauri::command]
pub async fn github_start_device_flow() -> Result<DeviceFlowResponse, CommandError> {
    log::info!("[github_start_device_flow] starting device flow");
    let client = reqwest::Client::new();
    let client_id = github_client_id();

    let response = client
        .post("https://github.com/login/device/code")
        .header("Accept", "application/json")
        .form(&[("client_id", client_id.as_str()), ("scope", "repo,read:user")])
        .send()
        .await
        .map_err(|e| {
            log::error!("[github_start_device_flow] Failed to start device flow: {e}");
            CommandError::External(format!("Failed to start device flow: {e}"))
        })?;

    let status = response.status();
    let body: serde_json::Value = response.json().await.map_err(|e| {
        log::error!("[github_start_device_flow] Failed to parse device flow response: {e}");
        CommandError::External(format!("Failed to parse device flow response: {e}"))
    })?;

    if !status.is_success() {
        let message = body["error_description"]
            .as_str()
            .or_else(|| body["error"].as_str())
            .unwrap_or("Unknown error");
        let err = format!("GitHub device flow error ({}): {}", status, message);
        log::error!("[github_start_device_flow] {err}");
        return Err(CommandError::External(err));
    }

    let device_code = body["device_code"]
        .as_str()
        .ok_or_else(|| CommandError::External("Missing device_code in response".into()))?
        .to_string();
    let user_code = body["user_code"]
        .as_str()
        .ok_or_else(|| CommandError::External("Missing user_code in response".into()))?
        .to_string();
    let verification_uri = body["verification_uri"]
        .as_str()
        .ok_or_else(|| CommandError::External("Missing verification_uri in response".into()))?
        .to_string();
    let expires_in = body["expires_in"]
        .as_u64()
        .ok_or_else(|| CommandError::External("Missing expires_in in response".into()))?;
    let interval = body["interval"].as_u64().unwrap_or(5);

    Ok(DeviceFlowResponse {
        device_code,
        user_code,
        verification_uri,
        expires_in,
        interval,
    })
}

/// Poll GitHub for the access token using the device code.
/// Returns Pending while the user hasn't authorized, SlowDown if polling too fast,
/// or Success with the user profile once authorized.
#[tauri::command]
pub async fn github_poll_for_token(
    state: State<'_, DbState>,
    device_code: String,
) -> Result<GitHubAuthResult, CommandError> {
    log::info!("[github_poll_for_token] polling for token");
    let client = reqwest::Client::new();
    let client_id = github_client_id();

    let response = client
        .post("https://github.com/login/oauth/access_token")
        .header("Accept", "application/json")
        .form(&[
            ("client_id", client_id.as_str()),
            ("device_code", device_code.as_str()),
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
        ])
        .send()
        .await
        .map_err(|e| {
            log::error!("[github_poll_for_token] Failed to poll for token: {e}");
            CommandError::External(format!("Failed to poll for token: {e}"))
        })?;

    let body: serde_json::Value = response.json().await.map_err(|e| {
        log::error!("[github_poll_for_token] Failed to parse token response: {e}");
        CommandError::External(format!("Failed to parse token response: {e}"))
    })?;

    if let Some(error) = body["error"].as_str() {
        return match error {
            "authorization_pending" => Ok(GitHubAuthResult::Pending),
            "slow_down" => Ok(GitHubAuthResult::SlowDown),
            _ => {
                let description = body["error_description"]
                    .as_str()
                    .unwrap_or("Unknown error");
                let err = format!("GitHub OAuth error: {} — {}", error, description);
                log::error!("[github_poll_for_token] {err}");
                Err(CommandError::External(err))
            }
        };
    }

    let access_token = body["access_token"]
        .as_str()
        .ok_or_else(|| CommandError::External("Missing access_token in response".into()))?
        .to_string();

    let auth_client = github_authenticated_client(&access_token);
    let user = fetch_github_user(&auth_client)
        .await
        .map_err(|e| {
            log::error!("[github_poll_for_token] failed to fetch user profile: {e}");
            e
        })?;

    {
        let conn = state.conn()?;
        let mut settings = crate::db::read_settings(&conn)?;
        settings.github_user_login = Some(user.login.clone());
        settings.github_user_avatar = Some(user.avatar_url.clone());
        settings.github_user_email = user.email.clone();
        settings.github_oauth_token = Some(access_token);
        crate::db::write_settings(&conn, &settings).inspect_err(|e| {
            log::error!("[github_poll_for_token] failed to save settings: {e}");
        })?;
    }

    log::info!("[github_poll_for_token] signed in as {}", user.login);
    Ok(GitHubAuthResult::Success { user })
}

/// Get the currently authenticated GitHub user from the database.
/// Returns None if not signed in.
#[tauri::command]
pub fn github_get_user(state: State<'_, DbState>) -> Result<Option<GitHubUser>, CommandError> {
    log::info!("[github_get_user]");
    let conn = state.conn()?;
    let settings = crate::db::read_settings(&conn)?;

    if settings.github_oauth_token.is_some() {
        let login = settings.github_user_login.unwrap_or_default();
        let avatar_url = settings.github_user_avatar.unwrap_or_default();
        let email = settings.github_user_email;
        Ok(Some(GitHubUser {
            login,
            avatar_url,
            email,
        }))
    } else {
        Ok(None)
    }
}

/// Sign out of GitHub by clearing all OAuth fields from the database.
#[tauri::command]
pub fn github_logout(state: State<'_, DbState>) -> Result<(), CommandError> {
    log::info!("[github_logout]");
    let conn = state.conn()?;
    let mut settings = crate::db::read_settings(&conn)?;
    settings.github_oauth_token = None;
    settings.github_user_login = None;
    settings.github_user_avatar = None;
    settings.github_user_email = None;
    crate::db::write_settings(&conn, &settings)?;
    Ok(())
}

#[tauri::command]
pub async fn github_list_repos(
    state: State<'_, DbState>,
    query: String,
    limit: Option<usize>,
) -> Result<Vec<GitHubRepo>, CommandError> {
    log::info!("[github_list_repos] query={}", query);
    let token = {
        let conn = state.conn()?;
        let settings = crate::db::read_settings(&conn)?;
        settings
            .github_oauth_token
            .ok_or_else(|| CommandError::Validation("GitHub is not connected".into()))?
    };

    let client = github_authenticated_client(&token);
    let response = client
        .get("https://api.github.com/user/repos")
        .query(&[
            ("per_page", "100"),
            ("sort", "updated"),
            ("affiliation", "owner,collaborator,organization_member"),
        ])
        .send()
        .await
        .map_err(|e| {
            log::error!("[github_list_repos] Failed to list GitHub repos: {e}");
            CommandError::External(format!("Failed to list GitHub repos: {e}"))
        })?;

    let status = response.status();
    let body: serde_json::Value = response.json().await.map_err(|e| {
        log::error!("[github_list_repos] Failed to parse repo list response: {e}");
        CommandError::External(format!("Failed to parse repo list response: {e}"))
    })?;

    if !status.is_success() {
        let message = body["message"].as_str().unwrap_or("Unknown error");
        let err = format!("GitHub API error listing repos ({}): {}", status, message);
        log::error!("[github_list_repos] {err}");
        return Err(CommandError::External(err));
    }

    let query_lc = query.to_lowercase();
    let max = limit.unwrap_or(10).min(100);
    let repos = body
        .as_array()
        .ok_or_else(|| CommandError::External("Unexpected response format from GitHub".into()))?
        .iter()
        .filter_map(|repo| {
            let id = repo["id"].as_i64()?;
            let full_name = repo["full_name"].as_str()?.to_string();
            let clone_url = repo["clone_url"].as_str()?.to_string();
            let private = repo["private"].as_bool().unwrap_or(false);
            if !query_lc.is_empty() && !full_name.to_lowercase().contains(&query_lc) {
                return None;
            }
            Some(GitHubRepo {
                id,
                full_name,
                clone_url,
                private,
            })
        })
        .take(max)
        .collect::<Vec<_>>();

    Ok(repos)
}

/// Send a GET request via an authenticated GitHub API client.
/// Returns the response on success. Maps network errors to `CommandError::External`
/// with the provided `context` label for logging.
async fn github_api_get(
    client: &reqwest::Client,
    url: &str,
    context: &str,
) -> Result<reqwest::Response, CommandError> {
    client.get(url).send().await.map_err(|e| {
        log::error!("[{context}] GET {url} failed: {e}");
        CommandError::External(format!("{context}: {e}"))
    })
}

/// Parse a non-success GitHub API response into a `CommandError`.
async fn github_api_error(resp: reqwest::Response, context: &str) -> CommandError {
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    let msg = format!(
        "{} ({}): {}",
        context,
        status,
        body["message"].as_str().unwrap_or("unknown")
    );
    log::error!("[github_check_repo_empty] {msg}");
    CommandError::External(msg)
}

/// Check whether a GitHub repo is suitable as a migration target.
/// Returns true if the repo has no directories at its root (project folders).
/// A repo with only top-level files (e.g. a README) is still considered suitable.
#[tauri::command]
pub async fn github_check_repo_empty(
    state: State<'_, DbState>,
    full_name: String,
) -> Result<bool, CommandError> {
    log::info!("[github_check_repo_empty] repo={}", full_name);
    let token = {
        let conn = state.conn()?;
        let settings = crate::db::read_settings(&conn)?;
        settings
            .github_oauth_token
            .ok_or_else(|| CommandError::Validation("GitHub is not connected".into()))?
    };

    let client = github_authenticated_client(&token);

    // First check branches. A 409 means the repo has no git history at all — definitely usable.
    let branches_url = format!("https://api.github.com/repos/{full_name}/branches");
    let branches_resp = github_api_get(&client, &branches_url, "github_check_repo_empty").await?;

    let branches_status = branches_resp.status();
    if branches_status.as_u16() == 409 {
        log::info!("[github_check_repo_empty] repo={} status=no_git_history (409)", full_name);
        return Ok(true);
    }
    if !branches_status.is_success() {
        return Err(github_api_error(branches_resp, "GitHub API error checking branches").await);
    }

    // Repo has at least one branch. Check root contents for any directory (project folder).
    let contents_url = format!("https://api.github.com/repos/{full_name}/contents/");
    let contents_resp = github_api_get(&client, &contents_url, "github_check_repo_empty").await?;

    let contents_status = contents_resp.status();
    if contents_status.as_u16() == 404 {
        log::info!("[github_check_repo_empty] repo={} status=no_contents", full_name);
        return Ok(true);
    }
    if !contents_status.is_success() {
        return Err(github_api_error(contents_resp, "GitHub API error checking contents").await);
    }

    let contents: serde_json::Value = contents_resp.json().await.map_err(|e| {
        CommandError::External(format!("Failed to parse repo contents: {e}"))
    })?;
    let has_project_folder = contents
        .as_array()
        .map(|items| items.iter().any(|item| item["type"].as_str() == Some("dir")))
        .unwrap_or(false);

    log::info!(
        "[github_check_repo_empty] repo={} has_project_folder={}",
        full_name,
        has_project_folder
    );
    Ok(!has_project_folder)
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::types::AppSettings;

    #[test]
    fn settings_roundtrip_persists_github_fields() {
        let conn = db::open_in_memory().unwrap();
        let settings = AppSettings {
            github_oauth_token: Some("tok_abc".to_string()),
            github_user_login: Some("octocat".to_string()),
            github_user_avatar: Some("https://github.com/octocat.png".to_string()),
            github_user_email: Some("octocat@github.com".to_string()),
            ..AppSettings::default()
        };
        db::write_settings(&conn, &settings).unwrap();
        let read = db::read_settings(&conn).unwrap();
        assert_eq!(read.github_oauth_token.as_deref(), Some("tok_abc"));
        assert_eq!(read.github_user_login.as_deref(), Some("octocat"));
        assert_eq!(
            read.github_user_avatar.as_deref(),
            Some("https://github.com/octocat.png")
        );
        assert_eq!(
            read.github_user_email.as_deref(),
            Some("octocat@github.com")
        );
    }

    #[test]
    fn read_settings_returns_default_when_empty() {
        let conn = db::open_in_memory().unwrap();
        let settings = db::read_settings(&conn).unwrap();
        assert!(settings.github_oauth_token.is_none());
        assert!(settings.github_user_login.is_none());
    }

    #[test]
    fn logout_clears_github_fields() {
        let conn = db::open_in_memory().unwrap();
        let settings = AppSettings {
            github_oauth_token: Some("tok_abc".to_string()),
            github_user_login: Some("octocat".to_string()),
            github_user_avatar: Some("https://github.com/octocat.png".to_string()),
            ..AppSettings::default()
        };
        db::write_settings(&conn, &settings).unwrap();

        // Simulate logout logic
        let mut s = db::read_settings(&conn).unwrap();
        s.github_oauth_token = None;
        s.github_user_login = None;
        s.github_user_avatar = None;
        s.github_user_email = None;
        db::write_settings(&conn, &s).unwrap();

        let after = db::read_settings(&conn).unwrap();
        assert!(after.github_oauth_token.is_none());
        assert!(after.github_user_login.is_none());
    }

    #[test]
    fn get_user_returns_none_when_no_token() {
        let conn = db::open_in_memory().unwrap();
        // No token stored → user should be None
        let settings = db::read_settings(&conn).unwrap();
        let user = if settings.github_oauth_token.is_some() {
            Some(crate::types::GitHubUser {
                login: settings.github_user_login.unwrap_or_default(),
                avatar_url: settings.github_user_avatar.unwrap_or_default(),
                email: settings.github_user_email,
            })
        } else {
            None
        };
        assert!(user.is_none());
    }

    #[test]
    fn get_user_returns_some_when_token_present() {
        let conn = db::open_in_memory().unwrap();
        let settings = AppSettings {
            github_oauth_token: Some("tok".to_string()),
            github_user_login: Some("dev".to_string()),
            github_user_avatar: Some("https://avatars.githubusercontent.com/u/1".to_string()),
            ..AppSettings::default()
        };
        db::write_settings(&conn, &settings).unwrap();
        let s = db::read_settings(&conn).unwrap();
        let user = if s.github_oauth_token.is_some() {
            Some(crate::types::GitHubUser {
                login: s.github_user_login.unwrap_or_default(),
                avatar_url: s.github_user_avatar.unwrap_or_default(),
                email: s.github_user_email,
            })
        } else {
            None
        };
        let user = user.unwrap();
        assert_eq!(user.login, "dev");
        assert!(user.email.is_none());
    }
}

async fn fetch_github_user(client: &reqwest::Client) -> Result<GitHubUser, CommandError> {
    let response = client
        .get("https://api.github.com/user")
        .send()
        .await
        .map_err(|e| {
            log::error!("[fetch_github_user] Failed to fetch GitHub user: {e}");
            CommandError::External(format!("Failed to fetch GitHub user: {e}"))
        })?;

    let status = response.status();
    let body: serde_json::Value = response.json().await.map_err(|e| {
        log::error!("[fetch_github_user] Failed to parse GitHub user response: {e}");
        CommandError::External(format!("Failed to parse GitHub user response: {e}"))
    })?;

    if !status.is_success() {
        let message = body["message"].as_str().unwrap_or("Unknown error");
        let err = format!("GitHub API error fetching user ({}): {}", status, message);
        log::error!("[fetch_github_user] {err}");
        return Err(CommandError::External(err));
    }

    let login = body["login"]
        .as_str()
        .ok_or_else(|| CommandError::External("Missing login in user response".into()))?
        .to_string();
    let avatar_url = body["avatar_url"]
        .as_str()
        .ok_or_else(|| CommandError::External("Missing avatar_url in user response".into()))?
        .to_string();
    let email = body["email"].as_str().map(|s| s.to_string());

    Ok(GitHubUser {
        login,
        avatar_url,
        email,
    })
}
