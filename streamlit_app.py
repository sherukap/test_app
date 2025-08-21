import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import tempfile
import os

# ===== CONFIGURATION =====
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}


def upload_to_stage(session, original_filename, df):
    """Uploads CSV with original filename"""
    try:
        # Create temp file with ORIGINAL filename structure
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, original_filename)
        
        # Save CSV with original name
        df.to_csv(temp_path, index=False)
        
        # Upload with original filename
        result = session.file.put(
            local_file_name=temp_path,
            stage_location="@MY_FILES",
            overwrite=True,
            auto_compress=False
        )
        
        # Verify the specific file uploaded
        files = session.sql(f"LIST @MY_FILES PATTERN='.*{original_filename}'").collect()
        return len(files) > 0
        
    except Exception as e:
        st.error(f"Upload error: {str(e)}")
        return False
    finally:
        # Cleanup
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.remove(temp_path)
        if 'temp_dir' in locals() and os.path.exists(temp_dir):
            os.rmdir(temp_dir)

def main():
    st.title("📤 CSV Upload with Original Filenames")
    
    uploaded_file = st.file_uploader("Choose CSV", type="csv")
    
    if uploaded_file:
        session = Session.builder.configs(SNOWFLAKE_CONFIG).create()
        try:
            df = pd.read_csv(uploaded_file)
            st.dataframe(df.head(3))
            
            if st.button("Upload"):
                if upload_to_stage(session, uploaded_file.name, df):
                    st.success(f"✅ Uploaded as: {uploaded_file.name}")
                    # Show stage contents
                    files = session.sql("LIST @MY_FILES").collect()
                    st.table([f.name for f in files])
                else:
                    st.error("Upload failed")
        finally:
            session.close()

if __name__ == "__main__":
    main()
