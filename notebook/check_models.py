#!/usr/bin/env python3
"""
Script kiểm tra model đã được train chưa và test nhanh prediction
"""

import os
import sys

def check_models():
    """Kiểm tra models đã được train"""
    print("="*80)
    print("🔍 CHECKING TRAINED MODELS")
    print("="*80)
    print()
    
    # Check GBT model
    gbt_path = "/user/jovyan/gbt_fraud_model"
    if os.path.exists(gbt_path):
        files = os.listdir(gbt_path)
        print(f"✅ GBT Model found: {gbt_path}/")
        print(f"   Files: {len(files)} files")
        print(f"   Size: {sum(os.path.getsize(os.path.join(gbt_path, f)) for f in files if os.path.isfile(os.path.join(gbt_path, f)))/1024/1024:.2f} MB")
    else:
        print(f"❌ GBT Model NOT found: {gbt_path}/")
        print("   👉 Chạy train.ipynb để train model!")
        return False
    
    print()
    
    # Check Scaler model
    scaler_path = "spark_scaler_model"
    if os.path.exists(scaler_path):
        files = os.listdir(scaler_path)
        print(f"✅ Scaler Model found: {scaler_path}/")
        print(f"   Files: {len(files)} files")
        print(f"   Size: {sum(os.path.getsize(os.path.join(scaler_path, f)) for f in files if os.path.isfile(os.path.join(scaler_path, f)))/1024/1024:.2f} MB")
    else:
        print(f"❌ Scaler Model NOT found: {scaler_path}/")
        print("   👉 Chạy train.ipynb để train model!")
        return False
    
    print()
    print("="*80)
    print("✅ TẤT CẢ MODELS ĐÃ SẴN SÀNG!")
    print("="*80)
    return True


def test_prediction():
    """Test quick prediction với sample data"""
    print()
    print("="*80)
    print("🧪 TESTING PREDICTION")
    print("="*80)
    print()
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.ml.classification import GBTClassificationModel
        from pyspark.ml.feature import VectorAssembler, StandardScalerModel
        from pyspark.sql.types import *
        
        # Tạo Spark session
        print("🔧 Khởi tạo Spark Session...")
        spark = SparkSession.builder \
            .appName("Model Test") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        # Load models
        print("📦 Loading models...")
        gbt_model = GBTClassificationModel.load("gbt_fraud_model")
        scaler_model = StandardScalerModel.load("spark_scaler_model")
        print("✅ Models loaded successfully!")
        
        # Tạo sample data (1 transaction)
        print()
        print("📊 Creating sample transaction...")
        
        schema = StructType([
            StructField("type_encoded", IntegerType(), False),
            StructField("amount_log", DoubleType(), False),
            StructField("errorBalanceOrig", DoubleType(), False),
            StructField("errorBalanceDest", DoubleType(), False),
            StructField("amount_over_oldbalance", DoubleType(), False),
            StructField("hour", IntegerType(), False)
        ])
        
        # Sample: Suspicious transaction (high amount, errors)
        sample_data = [(1, 12.5, 5000.0, -5000.0, 2.5, 3)]
        df = spark.createDataFrame(sample_data, schema)
        
        print("   Type: TRANSFER (encoded=1)")
        print("   Amount: ~$270,000 (log=12.5)")
        print("   Hour: 3 AM (suspicious)")
        
        # Create features vector
        feature_cols = ['type_encoded', 'amount_log', 'errorBalanceOrig', 
                       'errorBalanceDest', 'amount_over_oldbalance', 'hour']
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        df_assembled = assembler.transform(df)
        
        # Scale
        df_scaled = scaler_model.transform(df_assembled)
        
        # Predict
        print()
        print("🎯 Making prediction...")
        predictions = gbt_model.transform(df_scaled)
        
        # Get result
        result = predictions.select("prediction", "probability").collect()[0]
        pred = int(result.prediction)
        prob = result.probability[1]
        
        print()
        print("="*80)
        print("📊 PREDICTION RESULT")
        print("="*80)
        print(f"Prediction: {'🚨 FRAUD' if pred == 1 else '✅ NORMAL'}")
        print(f"Fraud Probability: {prob:.4f} ({prob*100:.2f}%)")
        print("="*80)
        
        spark.stop()
        return True
        
    except ImportError as e:
        print(f"❌ Lỗi import: {e}")
        print("   Đảm bảo PySpark đã được cài đặt")
        return False
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    # Check models exist
    if not check_models():
        print()
        print("⚠️  Cần train model trước!")
        print("   👉 Mở và chạy notebook: train.ipynb")
        sys.exit(1)
    
    # Test prediction
    print()
    choice = input("Bạn có muốn test prediction không? (y/n): ")
    if choice.lower() == 'y':
        test_prediction()
    
    print()
    print("✅ Kiểm tra hoàn tất!")
    print()


if __name__ == "__main__":
    main()
