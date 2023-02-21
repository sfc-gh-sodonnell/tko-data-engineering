#utilities
from cachetools import cached
import os
import sys
import json
import zipfile


import nltk
nltk.download('vader_lexicon');
from nltk.sentiment import SentimentIntensityAnalyzer




#snowflake packages
from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import *

    
# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()
    session.sql('USE ROLE ACCOUNTADMIN').show()
    session.sql('USE DATABASE ROARK_DEMO').show()
    session.sql('USE SCHEMA PUBLIC').show()
    #create a stage to host the udf
    session.sql('CREATE OR REPLACE STAGE model_stage').show()

    #Sentiment UDF
    sentiment = SentimentIntensityAnalyzer()
    def get_sentiment(text):
        return sentiment.polarity_scores(text)["compound"]

    #create model udf
    get_sentiment_udf = session.udf.register(lambda *args: 
                                        get_sentiment(str(args)),
                                        name="get_sentiment",
                                        stage_location="@model_stage",
                                        return_type=FloatType(),
                                        is_permanent=True,
                                        input_types=[StringType()],
                                        packages=["nltk"],
                                        replace=True)
    
    #    validate_raw_tables(session)

    session.close()
