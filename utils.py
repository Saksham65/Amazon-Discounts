import pathway as pw
import json

@pw.udf
def as_json_string(doc: str) -> bytes:
    """
    Convert the 'doc' field, which is a string representation of a dictionary,
    into a properly formatted JSON string and then encode it as bytes.
    """
    try:
        # Convert string representation of a dictionary into a proper JSON object
        parsed_doc = eval(doc)  # Convert string to dictionary (Ensure safety)
        json_doc = json.dumps(parsed_doc)  # Convert dictionary to JSON string
        return json_doc.encode("utf-8")  # Encode to bytes
    except Exception as e:
        return f'{{"error": "Failed to parse JSON: {str(e)}"}}'.encode("utf-8")


def stringify_table(t: pw.Table) -> pw.Table:
    """
    Apply the as_json_string UDF to the 'doc' column and store the result in a new column 'data'.
    """
    return t.with_columns(
        data=as_json_string(pw.this.doc)
    )


