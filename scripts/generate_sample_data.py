"""
Generate sample data for Tabby DWH project
"""
import os
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)
Faker.seed(42)

# Initialize faker
fake = Faker(['en_US', 'ar_SA'])

# Create directories if they don't exist
os.makedirs('data/raw', exist_ok=True)

def generate_customers(num_customers=1000):
    """Generate sample customer data"""
    customers = []
    countries = ['UAE', 'KSA', 'Egypt', 'Kuwait']
    country_codes = {'UAE': '+971', 'KSA': '+966', 'Egypt': '+20', 'Kuwait': '+965'}
    
    for i in range(num_customers):
        customer_id = f"CUST{i:06d}"
        country = random.choice(countries)
        phone_prefix = country_codes[country]
        
        # Create registration date within the last 2 years
        registration_date = fake.date_time_between(start_date='-2y', end_date='now')
        
        customer = {
            'customer_id': customer_id,
            'email': fake.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'phone_number': f"{phone_prefix}{fake.numerify(text='#######')}",
            'country': country,
            'city': fake.city(),
            'registration_date': registration_date,
            'last_login_date': fake.date_time_between(start_date=registration_date, end_date='now') if random.random() > 0.2 else None,
            'status': random.choice(['active', 'inactive', 'suspended']) if random.random() > 0.9 else 'active'
        }
        customers.append(customer)
    
    df = pd.DataFrame(customers)
    df.to_csv('data/raw/customers.csv', index=False)
    print(f"Generated {num_customers} customers")
    return df

def generate_merchants(num_merchants=100):
    """Generate sample merchant data"""
    merchants = []
    categories = ['Fashion', 'Electronics', 'Home & Garden', 'Beauty', 'Sports', 'Grocery', 'Restaurants', 'Travel']
    
    for i in range(num_merchants):
        merchant_id = f"MERCH{i:04d}"
        
        # Create onboarding date within the last 3 years
        onboarding_date = fake.date_time_between(start_date='-3y', end_date='-1m')
        
        merchant = {
            'merchant_id': merchant_id,
            'merchant_name': fake.company(),
            'category': random.choice(categories),
            'country': random.choice(['UAE', 'KSA', 'Egypt', 'Kuwait']),
            'integration_type': random.choice(['direct', 'marketplace', 'platform']),
            'onboarding_date': onboarding_date,
            'status': random.choice(['active', 'inactive', 'pending']) if random.random() > 0.9 else 'active'
        }
        merchants.append(merchant)
    
    df = pd.DataFrame(merchants)
    df.to_csv('data/raw/merchants.csv', index=False)
    print(f"Generated {num_merchants} merchants")
    return df

def generate_transactions(customers_df, merchants_df, num_transactions=10000):
    """Generate sample transaction data"""
    transactions = []
    payment_methods = ['credit_card', 'debit_card', 'bank_transfer']
    statuses = ['completed', 'pending', 'failed', 'cancelled', 'refunded']
    status_weights = [0.85, 0.05, 0.05, 0.03, 0.02]  # 85% completed, 5% pending, etc.
    
    # Get all customer and merchant IDs
    customer_ids = customers_df['customer_id'].tolist()
    merchant_ids = merchants_df['merchant_id'].tolist()
    
    # Start date for transactions
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(num_transactions):
        transaction_id = f"TXN{i:08d}"
        
        # Randomly select customer and merchant
        customer_id = random.choice(customer_ids)
        merchant_id = random.choice(merchant_ids)
        
        # Generate transaction date
        transaction_date = fake.date_time_between(start_date=start_date, end_date='now')
        
        # Generate amounts based on normal distribution
        amount = max(10, np.random.normal(loc=500, scale=300))
        amount = round(amount, 2)
        
        # Generate transaction
        transaction = {
            'transaction_id': transaction_id,
            'customer_id': customer_id,
            'merchant_id': merchant_id,
            'transaction_date': transaction_date,
            'amount': amount,
            'currency': random.choice(['AED', 'SAR', 'EGP', 'KWD']),
            'payment_method': random.choice(payment_methods),
            'status': random.choices(statuses, weights=status_weights)[0]
        }
        transactions.append(transaction)
    
    df = pd.DataFrame(transactions)
    df.to_csv('data/raw/transactions.csv', index=False)
    print(f"Generated {num_transactions} transactions")
    return df

def generate_payment_plans(transactions_df, num_plans=3000):
    """Generate sample payment plan data"""
    payment_plans = []
    
    # Filter for completed transactions
    completed_txns = transactions_df[transactions_df['status'] == 'completed']
    
    # Randomly select some transactions for payment plans
    selected_txns = completed_txns.sample(min(num_plans, len(completed_txns)))
    
    for i, txn in selected_txns.iterrows():
        plan_id = f"PLAN{i:06d}"
        
        # Generate payment plan
        installments = random.choice([3, 6, 12])
        total_amount = txn['amount']
        first_installment = round(total_amount / installments, 2)
        
        plan = {
            'plan_id': plan_id,
            'transaction_id': txn['transaction_id'],
            'customer_id': txn['customer_id'],
            'merchant_id': txn['merchant_id'],
            'plan_date': txn['transaction_date'],
            'total_amount': total_amount,
            'installment_count': installments,
            'first_installment_amount': first_installment,
            'status': random.choice(['active', 'completed', 'defaulted']) if random.random() > 0.9 else 'active'
        }
        payment_plans.append(plan)
    
    df = pd.DataFrame(payment_plans)
    df.to_csv('data/raw/payment_plans.csv', index=False)
    print(f"Generated {len(payment_plans)} payment plans")
    return df

def generate_installments(payment_plans_df):
    """Generate sample installment data"""
    installments = []
    
    for i, plan in payment_plans_df.iterrows():
        plan_id = plan['plan_id']
        plan_date = plan['plan_date']
        installment_count = plan['installment_count']
        total_amount = plan['total_amount']
        amount_per_installment = round(total_amount / installment_count, 2)
        
        for j in range(installment_count):
            installment_id = f"INST{i:06d}_{j+1}"
            due_date = pd.to_datetime(plan_date) + pd.DateOffset(months=j)
            
            # Determine status based on due date
            if due_date > datetime.now():
                status = 'scheduled'
                paid_date = None
            else:
                # Some randomness in payment behavior
                if random.random() > 0.85:  # 15% chance of late payment or default
                    if random.random() > 0.7:  # 30% of problematic payments are defaults
                        status = 'defaulted'
                        paid_date = None
                    else:
                        status = 'paid_late'
                        days_late = random.randint(1, 30)
                        paid_date = due_date + pd.DateOffset(days=days_late)
                else:
                    status = 'paid'
                    days_early = random.randint(0, 5)
                    paid_date = due_date - pd.DateOffset(days=days_early)
            
            installment = {
                'installment_id': installment_id,
                'plan_id': plan_id,
                'installment_number': j + 1,
                'amount': amount_per_installment,
                'due_date': due_date,
                'paid_date': paid_date,
                'status': status
            }
            installments.append(installment)
    
    df = pd.DataFrame(installments)
    df.to_csv('data/raw/installments.csv', index=False)
    print(f"Generated {len(installments)} installments")
    return df

def generate_user_events(customers_df, merchants_df, num_events=20000):
    """Generate sample user event data"""
    events = []
    event_types = ['app_open', 'product_view', 'search', 'add_to_cart', 'checkout', 'purchase']
    platforms = ['android', 'ios', 'web']
    
    customer_ids = customers_df['customer_id'].tolist()
    merchant_ids = merchants_df['merchant_id'].tolist()
    
    # Start date for events
    start_date = datetime.now() - timedelta(days=90)
    
    for i in range(num_events):
        event_id = str(uuid.uuid4())
        event_timestamp = fake.date_time_between(start_date=start_date, end_date='now')
        
        # Randomly select customer (some events may not have customer ID)
        customer_id = random.choice(customer_ids) if random.random() > 0.2 else None
        
        # Generate event
        event = {
            'event_id': event_id,
            'event_timestamp': event_timestamp,
            'customer_id': customer_id,
            'event_type': random.choice(event_types),
            'platform': random.choice(platforms),
            'merchant_id': random.choice(merchant_ids) if random.random() > 0.3 else None,
            'session_id': str(uuid.uuid4()),
            'device_type': random.choice(['mobile', 'tablet', 'desktop']),
            'country': random.choice(['UAE', 'KSA', 'Egypt', 'Kuwait'])
        }
        events.append(event)
    
    df = pd.DataFrame(events)
    df.to_csv('data/raw/user_events.csv', index=False)
    print(f"Generated {num_events} user events")
    return df

def main():
    """Main function to generate all sample data"""
    print("Generating sample data for Tabby DWH project...")
    
    # Generate primary data
    customers_df = generate_customers(num_customers=1000)
    merchants_df = generate_merchants(num_merchants=100)
    transactions_df = generate_transactions(customers_df, merchants_df, num_transactions=10000)
    
    # Generate related data
    payment_plans_df = generate_payment_plans(transactions_df)
    generate_installments(payment_plans_df)
    generate_user_events(customers_df, merchants_df)
    
    print("Sample data generation complete!")

if __name__ == "__main__":
    main()
