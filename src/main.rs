use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Multipart, Path, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{post},
    BoxError, Router,
    Json,
};

use serde_json::json;
use futures::{Stream, TryStreamExt};
use std::io;
use tokio::{fs::File, io::BufWriter};
use tokio_util::io::StreamReader;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use aws_sdk_s3::primitives::ByteStream;
use serde::Serialize;
use tower_http::limit::RequestBodyLimitLayer;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=debug", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = aws_config::load_from_env().await;
    let s3_client = aws_sdk_s3::Client::new(&config);

    let app = Router::new()
        .route("/file/:file_name", post(save_request_body))
        .route("/upload", post(upload_hander))
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(10 * 1024 * 1023))
        .with_state(s3_client);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

// Handler that streams the request body to a file.
//
// POST'ing to `/file/foo.txt` will create a file called `foo.txt`.
async fn save_request_body(
    Path(file_name): Path<String>,
    request: Request,
) -> impl IntoResponse {
    stream_to_file(&file_name, request.into_body().into_data_stream()).await;
    Json(json!({"result": "ok"}))
}

// Save a `Stream` to a file
async fn stream_to_file<S, E>(path: &str, stream: S) -> Result<(), (StatusCode, String)>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<BoxError>,
{
    if !path_is_valid(path) {
        return Err((StatusCode::BAD_REQUEST, "Invalid path".to_owned()));
    }

    async {
        // Convert the stream into an `AsyncRead`.
        let body_with_io_error = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
        let body_reader = StreamReader::new(body_with_io_error);
        futures::pin_mut!(body_reader);

        // Create the file. `File` implements `AsyncWrite`.
        let path = std::path::Path::new("target").join(path);
        let mut file = BufWriter::new(File::create(path).await?);

        // Copy the body into the file.
        tokio::io::copy(&mut body_reader, &mut file).await?;

        Ok::<_, io::Error>(())
    }
    .await
    .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

// to prevent directory traversal attacks we ensure the path consists of exactly one normal
// component
fn path_is_valid(path: &str) -> bool {
    let path = std::path::Path::new(path);
    let mut components = path.components().peekable();

    if let Some(first) = components.peek() {
        if !matches!(first, std::path::Component::Normal(_)) {
            return false;
        }
    }

    components.count() == 1
}

#[derive(Serialize)]
struct S3File {
  key: String,
  successful: bool,
  url: String,
  file_name: String,
  content_type: String,
  #[serde(skip_serializing)]
  bytes: Bytes,
}

async fn upload_hander(
  State(s3_client): State<aws_sdk_s3::Client>,
  mut multipart: Multipart,
) -> Result<Response, Response> {
  let mut files = vec![];
  let bucket_name = std::env::var("AWS_BUCKET_NAME").unwrap_or_default();

  while let Some(field) = multipart
    .next_field()
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?
  {
    if let Some("files") = field.name() {
      let file_name = field.file_name().unwrap_or_default().to_owned();
      let content_type = field.file_name().unwrap_or_default().to_owned();
      let key = uuid::Uuid::new_v4().to_string();
      let url = format!("https://{bucket_name}.s3.amazonaws.com/{key}");

      let bytes = field
        .bytes()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;

      files.push(S3File {
        file_name,
        content_type,
        bytes,
        key,
        url,
        successful: false,
      })
    }
  }

  for file in &mut files {
    let body = ByteStream::from(file.bytes.to_vec());

    let res = s3_client
      .put_object()
      .bucket(&bucket_name)
      .content_type(&file.content_type)
      .content_length(file.bytes.len() as i64)
      .key(&file.key)
      .body(body)
      .send()
      .await;

    file.successful = res.is_ok();
  }

  Ok(
    (
      StatusCode::OK,
      [(header::CONTENT_TYPE, "application/json")],
      serde_json::json!(files).to_string(),
    )
      .into_response(),
  )
}
