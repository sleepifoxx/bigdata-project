from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Transaction(BaseModel):
    tx_id: int
    user_id: str
    amount: float
    oldbalanceOrg: float
    newbalanceOrg: float
    oldbalanceDest: float
    newbalanceDest: float
    type: str
    step: int

@app.post("/predict")
def predict(tx: Transaction):
    # dummy logic: đánh dấu fraud nếu amount > 1000
    is_fraud = tx.amount > 1000
    return {"is_fraud": is_fraud}
