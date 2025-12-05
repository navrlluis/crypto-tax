"""
Webhook for Spanish Crypto Tax Calculator
Receives CSV data from Google Form via Make.com
Returns IRPF + Modelo 721 summary
"""

from flask import Flask, request, jsonify
import os
import csv
import io
from datetime import datetime
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', 'dev-secret-change-me')
PORT = int(os.getenv('PORT', 5000))

# ============================================================================
# MODELOS SIMPLES (MVP)
# ============================================================================

class Transaction:
    """Representa una transacción de crypto"""
    
    def __init__(self, date, asset, tx_type, amount, price_eur, fee_eur=0):
        self.date = date
        self.asset = asset
        self.type = tx_type  # 'buy', 'sell', 'staking', 'transfer'
        self.amount = amount
        self.price_eur = price_eur
        self.fee_eur = fee_eur
    
    def total_cost(self):
        """Costo total incluyendo fees"""
        return (self.amount * self.price_eur) + self.fee_eur
    
    def __repr__(self):
        return f"TX({self.date}, {self.asset}, {self.type}, {self.amount})"


class SimpleTaxEngine:
    """
    Motor FIFO mínimo para MVP
    Calcula gains/losses básico
    """
    
    def __init__(self):
        self.transactions = []
        self.lots = {}  # {asset: [lot1, lot2, ...]}
        self.gains = 0
        self.losses = 0
        self.staking_income = 0
        self.errors = []
    
    def add_transaction(self, tx: Transaction):
        """Agregar transacción y actualizar state"""
        self.transactions.append(tx)
        
        if tx.type == 'buy':
            self._process_buy(tx)
        elif tx.type == 'sell':
            self._process_sell(tx)
        elif tx.type == 'staking':
            self._process_staking(tx)
        elif tx.type == 'transfer':
            pass  # No taxable
    
    def _process_buy(self, tx: Transaction):
        """Procesar compra: crear nuevo lot"""
        if tx.asset not in self.lots:
            self.lots[tx.asset] = []
        
        self.lots[tx.asset].append({
            'quantity': tx.amount,
            'cost_eur_total': tx.total_cost(),
            'cost_per_unit': tx.total_cost() / tx.amount if tx.amount > 0 else 0,
            'date': tx.date
        })
    
    def _process_sell(self, tx: Transaction):
        """Procesar venta: FIFO logic"""
        if tx.asset not in self.lots or not self.lots[tx.asset]:
            self.errors.append(f"Venta sin compra previa: {tx.asset}")
            return
        
        remaining = tx.amount
        proceeds = tx.amount * tx.price_eur
        cost_basis = 0
        
        while remaining > 0 and self.lots[tx.asset]:
            lot = self.lots[tx.asset][0]
            
            if lot['quantity'] <= remaining:
                # Vender todo el lot
                cost_basis += lot['cost_eur_total']
                remaining -= lot['quantity']
                self.lots[tx.asset].pop(0)
            else:
                # Vender parte del lot
                portion_cost = (lot['cost_per_unit'] * remaining)
                cost_basis += portion_cost
                lot['quantity'] -= remaining
                lot['cost_eur_total'] -= portion_cost
                remaining = 0
        
        # Calcular ganancia/pérdida
        gain = proceeds - cost_basis - tx.fee_eur
        
        if gain > 0:
            self.gains += gain
        else:
            self.losses += gain
    
    def _process_staking(self, tx: Transaction):
        """Procesar staking como income"""
        self.staking_income += tx.amount * tx.price_eur
    
    def get_summary(self):
        """Retornar resumen para email"""
        net_position = self.gains + self.losses
        
        return {
            'gains': round(self.gains, 2),
            'losses': round(abs(self.losses), 2),
            'net_position': round(net_position, 2),
            'staking_income': round(self.staking_income, 2),
            'total_transactions': len(self.transactions),
            'errors': self.errors,
            'estimated_tax_liability': self._estimate_tax(net_position)
        }
    
    def _estimate_tax(self, net_position):
        """Estimar impuestos (español, marginal)"""
        if net_position <= 6000:
            rate = 0.19
        elif net_position <= 50000:
            rate = 0.21
        elif net_position <= 200000:
            rate = 0.23
        else:
            rate = 0.27
        
        # Simplificado: no contamos pérdidas anteriores
        if net_position > 0:
            return round(net_position * rate, 2)
        return 0

# ============================================================================
# CSV PARSER (MVP - BINANCE ONLY)
# ============================================================================

def parse_binance_csv(csv_content: str):
    """
    Parse Binance CSV export
    
    Formato esperado (Binance export):
    Date(UTC),Pair,Type,Price,Executed Quantity,Amount
    
    o
    
    Date(UTC),Coin,Change,...
    """
    
    transactions = []
    
    try:
        reader = csv.DictReader(io.StringIO(csv_content))
        
        for row in reader:
            try:
                # Normalizar nombres de columnas
                date_str = row.get('Date(UTC)') or row.get('Date') or row.get('UTC_Time')
                coin = row.get('Coin') or row.get('Asset')
                change_str = row.get('Change') or row.get('Amount')
                operation = row.get('Operation') or row.get('Type') or 'unknown'
                
                if not all([date_str, coin, change_str]):
                    continue
                
                # Parse date
                date = datetime.strptime(date_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                
                # Parse cantidad
                change = float(change_str)
                amount = abs(change)
                
                # Parse precio (si existe)
                price_str = row.get('Price') or row.get('Value') or '0'
                price = float(price_str) if price_str else 0
                
                # Determinar tipo
                if 'buy' in operation.lower() or change > 0:
                    tx_type = 'buy'
                elif 'sell' in operation.lower() or change < 0:
                    tx_type = 'sell'
                elif 'stake' in operation.lower() or 'reward' in operation.lower():
                    tx_type = 'staking'
                else:
                    tx_type = 'transfer'
                
                tx = Transaction(
                    date=date,
                    asset=coin,
                    tx_type=tx_type,
                    amount=amount,
                    price_eur=price,
                    fee_eur=0
                )
                
                transactions.append(tx)
            
            except Exception as e:
                logger.warning(f"Error parsing row: {row}, error: {str(e)}")
                continue
    
    except Exception as e:
        logger.error(f"CSV parsing error: {str(e)}")
        return []
    
    return transactions

# ============================================================================
# ENDPOINTS
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    """Health check para Railway"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})


@app.route('/calculate', methods=['POST'])
def calculate():
    """
    Endpoint principal
    
    Recibe (vía Make.com):
    {
        "email": "user@example.com",
        "nif": "12345678A",
        "nombre": "Juan García",
        "csv_content": "Date(UTC),Coin,Change,...\n...",
        "exchange": "binance",
        "blockchains": ["ethereum", "polygon"]
    }
    """
    
    try:
        # Obtener datos
        data = request.get_json()
        
        if not data:
            return jsonify({
                'status': 'error',
                'message': 'No JSON data provided'
            }), 400
        
        email = data.get('email')
        nif = data.get('nif')
        nombre = data.get('nombre')
        csv_content = data.get('csv_content')
        exchange = data.get('exchange', 'binance').lower()
        
        logger.info(f"Processing for {email}, exchange: {exchange}")
        
        # Validar datos requeridos
        if not all([email, nif, nombre, csv_content]):
            return jsonify({
                'status': 'error',
                'message': 'Missing required fields: email, nif, nombre, csv_content'
            }), 400
        
        # Parse CSV (MVP: solo Binance)
        if exchange == 'binance':
            transactions = parse_binance_csv(csv_content)
        else:
            transactions = parse_binance_csv(csv_content)
        
        if not transactions:
            return jsonify({
                'status': 'error',
                'message': 'No transactions parsed from CSV'
            }), 400
        
        logger.info(f"Parsed {len(transactions)} transactions")
        
        # Ejecutar FIFO engine
        engine = SimpleTaxEngine()
        
        for tx in transactions:
            engine.add_transaction(tx)
        
        # Generar summary
        summary = engine.get_summary()
        
        logger.info(f"Calculation complete: gains={summary['gains']}, losses={summary['losses']}")
        
        return jsonify({
            'status': 'success',
            'email': email,
            'nombre': nombre,
            'nif': nif,
            'exchange': exchange,
            'summary': summary,
            'timestamp': datetime.now().isoformat()
        }), 200
    
    except Exception as e:
        logger.error(f"Calculation error: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@app.route('/', methods=['GET'])
def index():
    """Landing page"""
    return jsonify({
        'name': 'Spanish Crypto Tax Calculator',
        'version': '0.1.0',
        'endpoints': {
            '/health': 'GET - Health check',
            '/calculate': 'POST - Calculate taxes from CSV'
        }
    }), 200


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    logger.info(f"Starting webhook server on port {PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False)
