Bronze Bucket Name - yt-data-pipeline-bronze-eu-paris-dev
Silver Bucket Name - yt-data-pipeline-argent-eu-paris-dev
Gold Bucket Name - yt-data-pipeline-gold-eu-paris-dev

Scripts Bucket - yt-data-pipeline-scripts-eu-paris-dev

SNS ARN - arn:aws:sns:eu-west-3:<AWS_ACCOUNT_ID>:yt-data-pipeline-alerts-dev:<SNS_SUBSCRIPTION_ID>

CLAUDE (ARN + : + UUID) - arn:aws:sns:eu-west-3:<AWS_ACCOUNT_ID>:yt-data-pipeline-alerts-dev : <SNS_SUBSCRIPTION_ID>

Glue Bronze - yt_pipeline_bronze_dev
Glue Argent - yt_pipeline_argent_dev
Glue Gold - yt_pipeline_gold_dev

--bronze_database yt_pipeline_bronze_dev
--bronze_table raw_statistics
--silver_bucket yt-data-pipeline-argent-eu-paris-dev
--silver_database yt_pipeline_argent_dev
--silver_table clean_statistics

--silver_database yt_pipeline_argent_dev
--gold_bucket yt-data-pipeline-gold-eu-paris-dev
--gold_database yt_pipeline_gold_dev