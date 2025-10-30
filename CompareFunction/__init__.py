import logging
import azure.functions as func
from compare_parallel import process_parallel_blob

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("📥 Compare Function triggered.")

    try:
        req_body = req.get_json()
        source_blob = req_body.get("source_blob")
        target_blob = req_body.get("target_blob")
        pk_col = req_body.get("pk_col", "cr_commit_no")

        if not source_blob or not target_blob:
            return func.HttpResponse("Missing 'source_blob' or 'target_blob'", status_code=400)

        output_blob = process_parallel_blob(source_blob, target_blob, pk_col)
        return func.HttpResponse(f"✅ Comparison complete. Results written to: {output_blob}", status_code=200)

    except Exception as e:
        logging.exception("❌ Error during comparison.")
        return func.HttpResponse(str(e), status_code=500)
