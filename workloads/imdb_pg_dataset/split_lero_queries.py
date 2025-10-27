import os
import re

def split_queries(input_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Match blocks like: q0#####SELECT ...
    matches = re.findall(r"(q\d+)#####(.*?)(?=q\d+#####|$)", content, re.S)

    for query_id, query_sql in matches:
        # Remove tabs
        query_sql = query_sql.replace("\t", " ")

        # Collapse multiple spaces
        query_sql = re.sub(r" +", " ", query_sql)

        # Normalize spacing
        query_sql = re.sub(r"\s*\n\s*", " ", query_sql).strip()

        # Add newline before FROM and WHERE
        query_sql = re.sub(r"\s+(FROM)\s+", r"\nFROM ", query_sql, flags=re.I)
        query_sql = re.sub(r"\s+(WHERE)\s+", r"\nWHERE ", query_sql, flags=re.I)

        # Break each WHERE condition onto its own line
        if "WHERE" in query_sql.upper():
            query_sql = re.sub(
                r"\s+AND\s+", r"\n    AND ", query_sql, flags=re.I
            )
            query_sql = re.sub(
                r"\s+OR\s+", r"\n    OR ", query_sql, flags=re.I
            )

        filename = os.path.join(output_dir, f"{query_id}.sql")

        with open(filename, "w", encoding="utf-8") as out:
            out.write(query_sql.strip() + ";\n")

        print(f"Saved: {filename}")

if __name__ == "__main__":
    input_file = "/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/workloads/imdb_pg_dataset/job_lero.txt"   # Replace with your txt file
    output_dir = "/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/workloads/imdb_pg_dataset/job_lero"   # Directory for output .sql files
    split_queries(input_file, output_dir)
