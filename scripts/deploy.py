import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_POLL_SECONDS = 10
DEFAULT_MAX_WAIT_SECONDS = 60 * 60  # 1 hour
DEFAULT_MAX_RETRIES = 6

RETRYABLE_HTTP = {429, 500, 502, 503, 504}

def _redact(s: str) -> str:
    if not s:
        return s
    if len(s) <= 8:
        return "****"
    return s[:4] + "****" + s[-4:]

def _json_dumps(data: Any) -> bytes:
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def _request(
    method: str,
    url: str,
    headers: Dict[str, str],
    body: Optional[bytes],
    timeout: int,
    max_retries: int,
) -> Tuple[int, bytes]:
    """
    Basic retry with exponential backoff for transient failures and retryable HTTP codes.
    """
    attempt = 0
    last_err: Optional[Exception] = None

    while attempt <= max_retries:
        try:
            req = urllib.request.Request(url=url, data=body, method=method)
            for k, v in headers.items():
                req.add_header(k, v)

            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read()

        except urllib.error.HTTPError as e:
            status = getattr(e, "code", None)
            payload = e.read() if hasattr(e, "read") else b""
            # Retry only if retryable
            if status in RETRYABLE_HTTP and attempt < max_retries:
                sleep = min(60, (2 ** attempt))  # 1,2,4,8,16,32... capped
                print(f"[warn] HTTP {status} retryable. attempt={attempt+1}/{max_retries} sleep={sleep}s")
                time.sleep(sleep)
                attempt += 1
                continue
            # Non-retryable or out of retries
            raise RuntimeError(f"HTTPError {status}: {payload.decode('utf-8', errors='replace')}") from e

        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            if attempt < max_retries:
                sleep = min(60, (2 ** attempt))
                print(f"[warn] Network/timeout retry. attempt={attempt+1}/{max_retries} sleep={sleep}s err={e}")
                time.sleep(sleep)
                attempt += 1
                continue
            break

    raise RuntimeError(f"Request failed after retries: {last_err}") from last_err

def _make_idempotency_key(payload: Dict[str, Any], salt: str) -> str:
    raw = _json_dumps(payload) + salt.encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def _parse_json(b: bytes) -> Any:
    if not b:
        return {}
    return json.loads(b.decode("utf-8"))

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True, help="Path to payload JSON")
    ap.add_argument("--base-url", required=False, default=os.getenv("DEPLOY_API_BASE_URL", "").strip(),
                    help="Base URL, e.g. https://deploy-api.internal")
    ap.add_argument("--api-key", required=False, default=os.getenv("DEPLOY_API_KEY", "").strip(),
                    help="API key (prefer env var)")
    ap.add_argument("--timeout", type=int, default=int(os.getenv("DEPLOY_API_TIMEOUT", DEFAULT_TIMEOUT_SECONDS)))
    ap.add_argument("--poll", type=int, default=int(os.getenv("DEPLOY_API_POLL_SECONDS", DEFAULT_POLL_SECONDS)))
    ap.add_argument("--max-wait", type=int, default=int(os.getenv("DEPLOY_API_MAX_WAIT", DEFAULT_MAX_WAIT_SECONDS)))
    ap.add_argument("--max-retries", type=int, default=int(os.getenv("DEPLOY_API_MAX_RETRIES", DEFAULT_MAX_RETRIES)))
    ap.add_argument("--submit-path", default=os.getenv("DEPLOY_API_SUBMIT_PATH", "/deployments"))
    ap.add_argument("--status-path-template", default=os.getenv("DEPLOY_API_STATUS_PATH_TEMPLATE", "/deployments/{jobId}"))
    ap.add_argument("--correlation-id", default=os.getenv("BUILD_BUILDID", "") or os.getenv("SYSTEM_JOBID", "") or "")
    args = ap.parse_args()

    if not args.base_url:
        print("Missing base URL. Provide --base-url or set DEPLOY_API_BASE_URL.")
        return 2
    if not args.api_key:
        print("Missing API key. Provide --api-key or set DEPLOY_API_KEY.")
        return 2

    with open(args.payload, "r", encoding="utf-8") as f:
        payload = json.load(f)

    # Idempotency key salt ties to this pipeline run & commit where possible
    salt = (os.getenv("BUILD_SOURCEVERSION", "") + "|" + os.getenv("BUILD_BUILDID", "") + "|" + args.base_url)
    idem_key = _make_idempotency_key(payload, salt)

    submit_url = args.base_url.rstrip("/") + args.submit_path

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-API-Key": args.api_key,
        "Idempotency-Key": idem_key,
    }
    if args.correlation_id:
        headers["X-Correlation-Id"] = str(args.correlation_id)

    print(f"[info] Submitting deployment to: {submit_url}")
    print(f"[info] Using Idempotency-Key: {idem_key[:8]}…")
    print(f"[info] Using API key: {_redact(args.api_key)}")

    status, resp_bytes = _request(
        method="POST",
        url=submit_url,
        headers=headers,
        body=_json_dumps(payload),
        timeout=args.timeout,
        max_retries=args.max_retries,
    )

    resp = _parse_json(resp_bytes)
    job_id = resp.get("jobId") or resp.get("id") or resp.get("deploymentId")
    if not job_id:
        print(f"[error] Submit response missing job id. status={status} resp={resp}")
        return 3

    print(f"[info] jobId={job_id}")

    # Poll
    status_url = args.base_url.rstrip("/") + args.status_path_template.format(jobId=job_id)
    start = time.time()

    while True:
        elapsed = int(time.time() - start)
        if elapsed > args.max_wait:
            print(f"[error] Timed out waiting for job {job_id}. waited={elapsed}s")
            return 4

        s_status, s_bytes = _request(
            method="GET",
            url=status_url,
            headers={
                "Accept": "application/json",
                "X-API-Key": args.api_key,
                "X-Correlation-Id": headers.get("X-Correlation-Id", ""),
            },
            body=None,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )

        s_resp = _parse_json(s_bytes)
        state = (s_resp.get("status") or s_resp.get("state") or "").lower()

        # Normalize states you might return
        if state in ("succeeded", "success", "completed"):
            print(f"[info] Deployment SUCCEEDED for job {job_id}.")
            _write_artifacts(job_id, payload, resp, s_resp)
            return 0
        if state in ("failed", "error", "cancelled", "canceled"):
            print(f"[error] Deployment FAILED for job {job_id}. status_payload={s_resp}")
            _write_artifacts(job_id, payload, resp, s_resp)
            return 5

        # still running / queued / unknown
        msg = s_resp.get("message") or s_resp.get("summary") or ""
        print(f"[info] job={job_id} state={state or 'unknown'} elapsed={elapsed}s {msg}".rstrip())
        time.sleep(args.poll)

def _write_artifacts(job_id: str, payload: Dict[str, Any], submit_resp: Any, status_resp: Any) -> None:
    out_dir = os.getenv("BUILD_ARTIFACTSTAGINGDIRECTORY", "artifacts")
    os.makedirs(out_dir, exist_ok=True)

    path = os.path.join(out_dir, f"deploy-result-{job_id}.json")
    data = {
        "jobId": job_id,
        "payload": payload,
        "submitResponse": submit_resp,
        "finalStatus": status_resp,
        "timestampUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"[info] Wrote artifact: {path}")

if __name__ == "__main__":
    sys.exit(main())
