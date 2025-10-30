import pandas as pd
import threading, math, psutil, json, time, io
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

STORAGE_ACCOUNT_URL = "https://sa13.blob.core.windows.net"
CONTAINER_NAME = "testcontainer"

credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=credential)

def read_blob_to_df(blob_name: str):
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    blob_data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(blob_data), dtype=str)
    return df.fillna("").applymap(lambda v: v.strip() if isinstance(v, str) else v)

def write_results_to_blob(blob_name: str, content: str):
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    blob_client.upload_blob(content, overwrite=True)
    print(f"✅ Uploaded comparison result to blob: {blob_name}")
    return blob_name

def auto_tune_settings(total_records):
    cpu_count = psutil.cpu_count(logical=False) or 1
    mem_gb = psutil.virtual_memory().available / (1024 ** 3)
    if total_records <= 100_000:
        chunk_size = 20_000; threads = min(6, cpu_count)
    elif total_records <= 1_000_000:
        chunk_size = 30_000; threads = min(8, cpu_count * 2)
    else:
        chunk_size = 50_000; threads = min(16, cpu_count * 2)
    if mem_gb < 4:
        chunk_size = max(5_000, chunk_size // 2)
        threads = max(2, threads // 2)
    return chunk_size, threads

def compare_chunk(src_chunk, tgt_chunk, pk_col, chunk_id=None):
    start_ts = time.time()
    thread_name = threading.current_thread().name
    print(f"🔹 chunk_start id={chunk_id} rows={len(src_chunk)} thread={thread_name}")
    diffs, matched, missing, mismatched, checked = [], 0, 0, 0, 0

    def normalize(val):
        if pd.isna(val) or val is None: return ""
        if isinstance(val, (int, float)): val = str(int(val)) if str(val).endswith(".0") else str(val)
        return str(val).strip().replace("\xa0", "").replace("\r", "").replace("\t", "")

    tgt_map = {}
    if not tgt_chunk.empty:
        for _, row in tgt_chunk.iterrows():
            pk = normalize(row[pk_col])
            tgt_map[pk] = {col: normalize(row[col]) for col in tgt_chunk.columns}

    for _, src_row in src_chunk.iterrows():
        checked += 1
        pk_val = normalize(src_row[pk_col])
        tgt_row = tgt_map.get(pk_val)
        if tgt_row is None:
            missing += 1; diffs.append({"PK": pk_val, "Status": "MISSING_IN_TARGET"}); continue
        mismatch = {}
        for col in src_chunk.columns:
            if col == pk_col: continue
            if normalize(src_row[col]) != tgt_row.get(col, ""):
                mismatch[col] = {"src": normalize(src_row[col]), "tgt": tgt_row.get(col, "")}
        if mismatch:
            mismatched += 1; diffs.append({"PK": pk_val, "Status": "MISMATCH", "Diff": json.dumps(mismatch)})
        else:
            matched += 1

    print(f"🔸 chunk_done id={chunk_id} duration={time.time() - start_ts:.2f}s")
    return {"diffs": diffs, "matched": matched, "missing": missing, "mismatched": mismatched, "checked": checked}

def process_parallel_blob(source_blob: str, target_blob: str, pk_col: str):
    start_time = time.time()
    src_df = read_blob_to_df(source_blob)
    tgt_df = read_blob_to_df(target_blob)
    if pk_col not in src_df.columns or pk_col not in tgt_df.columns:
        raise KeyError(f"Primary key '{pk_col}' not found in one of the files")
    total_source, total_target = len(src_df), len(tgt_df)
    chunk_size, thread_count = auto_tune_settings(total_source)
    print(f"🧠 Auto-tuned: chunk_size={chunk_size}, thread_count={thread_count}")

    chunks = math.ceil(total_source / chunk_size)
    all_diffs, matched, missing, mismatched, checked = [], 0, 0, 0, 0

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = []
        for i in range(chunks):
            start, end = i * chunk_size, min((i + 1) * chunk_size, total_source)
            src_chunk = src_df.iloc[start:end]
            tgt_chunk = tgt_df[tgt_df[pk_col].isin(src_chunk[pk_col])]
            futures.append(executor.submit(compare_chunk, src_chunk, tgt_chunk, pk_col, i + 1))

        for i, f in enumerate(as_completed(futures), 1):
            res = f.result()
            all_diffs.extend(res["diffs"])
            matched += res["matched"]; missing += res["missing"]
            mismatched += res["mismatched"]; checked += res["checked"]
            print(f"✅ Chunk {i}/{chunks} done")

    summary = [
        f"Source count : {total_source}",
        f"Target count : {total_target}",
        f"Matched count : {matched}",
        f"Mismatched count : {mismatched}"
    ]
    content = "\n".join(summary)
    output_blob_name = f"output/diff_result_{int(time.time())}.txt"
    write_results_to_blob(output_blob_name, content)
    print(f"🕒 Total time: {time.time() - start_time:.2f}s")
    return output_blob_name
