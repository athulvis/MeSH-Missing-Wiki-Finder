import csv
import requests
from rapidfuzz.fuzz import token_sort_ratio
import time
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wikidata_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# User-Agent for Wikimedia compliance
HEADERS = {
    'User-Agent': 'WikidataProcessor/1.0 (https://example.com/contact) requests/2.31.0'
}

def search_wikidata_entities(keyword, language="en", limit=5):
    params = {
        "action": "wbsearchentities",
        "format": "json",
        "search": keyword,
        "language": language,
        "limit": limit,
    }
    try:
        r = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json().get("search", [])
    except Exception as e:
        logger.error(f"Error searching Wikidata for '{keyword}': {e}")
        return []

def get_wikipedia_link(qid, language="en"):
    params = {
        "action": "wbgetentities",
        "ids": qid,
        "format": "json",
        "props": "sitelinks/urls"
    }
    try:
        r = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        entities = r.json().get("entities", {})
        sitelinks = entities.get(qid, {}).get("sitelinks", {})
        site_key = f"{language}wiki"
        if site_key in sitelinks:
            return sitelinks[site_key].get("url")
        return ""
    except Exception as e:
        logger.error(f"Error fetching Wikipedia link for {qid}: {e}")
        return ""

def get_instances_and_subclasses(qid, language="en"):
    query = f"""
    SELECT ?p31 ?p31Label ?p279 ?p279Label WHERE {{
      OPTIONAL {{
        wd:{qid} wdt:P31 ?p31.
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],{language}". }}
      }}
      OPTIONAL {{
        wd:{qid} wdt:P279 ?p279.
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],{language}". }}
      }}
    }}
    """
    headers = {**HEADERS, 'Accept': 'application/sparql-results+json'}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = requests.get(SPARQL_ENDPOINT, params={'query': query}, headers=headers, timeout=30)

            if r.status_code == 429:
                logger.warning(f"Rate limited for {qid}, waiting 10 seconds...")
                time.sleep(10)
                continue

            r.raise_for_status()
            results = r.json()["results"]["bindings"]

            p31_qids = sorted({res["p31"]["value"].split("/")[-1] for res in results if "p31" in res})
            p31_labels = sorted({res["p31Label"]["value"] for res in results if "p31Label" in res})
            p279_qids = sorted({res["p279"]["value"].split("/")[-1] for res in results if "p279" in res})
            p279_labels = sorted({res["p279Label"]["value"] for res in results if "p279Label" in res})

            # Rate limiting - wait between SPARQL requests
            time.sleep(0.5)

            return p31_qids, p31_labels, p279_qids, p279_labels

        except Exception as e:
            logger.error(f"SPARQL error for {qid} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff

    return [], [], [], []

def get_processed_keywords(matched_csv, unmatched_csv):
    """Get set of already processed keywords from existing CSV files."""
    processed = set()

    for csv_file in [matched_csv, unmatched_csv]:
        if os.path.exists(csv_file):
            try:
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)  # Skip header
                    for row in reader:
                        if row:
                            processed.add(row[0].strip())
            except Exception as e:
                logger.error(f"Error reading existing file {csv_file}: {e}")

    return processed

def should_write_header(csv_file):
    """Check if CSV file is empty or doesn't exist (needs header)."""
    if not os.path.exists(csv_file):
        return True
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            return len(f.read().strip()) == 0
    except:
        return True

def process_keywords(input_csv, matched_csv, unmatched_csv, wikidata_limit=5, min_score=80):
    # Get already processed keywords for resumability
    processed_keywords = get_processed_keywords(matched_csv, unmatched_csv)
    logger.info(f"Found {len(processed_keywords)} already processed keywords")

    # Check if we need to write headers
    write_matched_header = should_write_header(matched_csv)
    write_unmatched_header = should_write_header(unmatched_csv)

    with open(input_csv, newline='', encoding='utf-8') as infile, \
         open(matched_csv, 'a', newline='', encoding='utf-8') as matchedfile, \
         open(unmatched_csv, 'a', newline='', encoding='utf-8') as unmatchedfile:

        reader = csv.reader(infile)
        matched_writer = csv.writer(matchedfile)
        unmatched_writer = csv.writer(unmatchedfile)

        # Write headers only if files are new/empty
        if write_matched_header:
            matched_writer.writerow([
                "Keyword", "Wikidata_QID", "Wikidata_Label", "Match_Score",
                "Instances_QIDs", "Instances_Labels",
                "Subclasses_QIDs", "Subclasses_Labels",
                "Wikipedia_English_Link"
            ])
            matchedfile.flush()

        if write_unmatched_header:
            unmatched_writer.writerow([
                "Keyword", "Wikidata_QID", "Wikidata_Label", "Match_Score",
                "Instances_QIDs", "Instances_Labels",
                "Subclasses_QIDs", "Subclasses_Labels",
                "Wikipedia_English_Link"
            ])
            unmatchedfile.flush()

        processed_count = 0
        skipped_count = 0

        for row in reader:
            if not row:
                continue
            keyword = row[0].strip()
            if not keyword:
                continue

            # Skip if already processed (resumability)
            if keyword in processed_keywords:
                skipped_count += 1
                continue

            try:
                entities = search_wikidata_entities(keyword, limit=wikidata_limit)
                matched_items = []
                unmatched_items = []

                for ent in entities:
                    # Ensure label is present, if not, fetch via wbgetentities as fallback
                    label = ent.get("label", "")
                    qid = ent.get("id", "")
                    if not label and qid:
                        # Fallback: fetch entity label via API
                        try:
                            p = {
                                "action": "wbgetentities",
                                "ids": qid,
                                "format": "json",
                                "props": "labels",
                                "languages": "en"
                            }
                            relabel_req = requests.get(WIKIDATA_API, params=p, headers=HEADERS, timeout=15)
                            relabel_req.raise_for_status()
                            label = relabel_req.json()["entities"].get(qid, {}).get("labels", {}).get("en", {}).get("value", "")
                        except Exception as e:
                            logger.error(f"Error fetching label for {qid}: {e}")
                            label = ""  # Still blank if not found

                    aliases = ent.get("aliases", [])
                    score_label = token_sort_ratio(keyword, label) if label else 0
                    scores_aliases = [token_sort_ratio(keyword, alias) for alias in aliases]
                    best_score = max([score_label] + scores_aliases) if (label or aliases) else 0

                    inst_qids, inst_labels, sub_qids, sub_labels = get_instances_and_subclasses(qid)
                    wiki_link = get_wikipedia_link(qid)

                    result_row = [
                        keyword,
                        qid,
                        label,
                        best_score,
                        "; ".join(inst_qids), "; ".join(inst_labels),
                        "; ".join(sub_qids), "; ".join(sub_labels),
                        wiki_link
                    ]

                    if best_score >= min_score:
                        matched_items.append(result_row)
                    else:
                        unmatched_items.append(result_row)

                # Write results immediately (on-the-go updates)
                if matched_items:
                    for row_data in matched_items:
                        matched_writer.writerow(row_data)
                    matchedfile.flush()  # Ensure data is written to disk
                elif unmatched_items:
                    for row_data in unmatched_items:
                        unmatched_writer.writerow(row_data)
                    unmatchedfile.flush()  # Ensure data is written to disk

                processed_count += 1
                logger.info(f"Processed keyword: {keyword} ({processed_count} processed, {skipped_count} skipped)")

            except Exception as e:
                logger.error(f"Error processing keyword '{keyword}': {e}")
                # Continue processing other keywords even if one fails

        logger.info(f"Processing complete. Total processed: {processed_count}, Total skipped: {skipped_count}")

# Usage
if __name__ == "__main__":
    process_keywords(
        input_csv='list_wiki.csv',
        matched_csv='matched_output.csv',
        unmatched_csv='unmatched_output.csv',
        wikidata_limit=5,
        min_score=80
    )
