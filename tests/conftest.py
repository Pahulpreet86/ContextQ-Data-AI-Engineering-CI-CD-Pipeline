import sys
from unittest.mock import MagicMock


sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.context"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
sys.modules["pyspark.sql.types"] = MagicMock()


sys.modules["awsglue"] = MagicMock()
sys.modules["awsglue.context"] = MagicMock()
sys.modules["awsglue.job"] = MagicMock()
sys.modules["awsglue.utils"] = MagicMock()
