import matplotlib.pyplot as plt
import os

def create_bar_chart():
    # --- ΒΑΛΕ ΕΔΩ ΤΑ ΔΙΚΑ ΣΟΥ ΔΕΔΟΜΕΝΑ ΑΠΟ ΤΟ ΤΕΡΜΑΤΙΚΟ ---
    # Παράδειγμα: ['JFK Airport', 'LaGuardia Airport', 'Midtown Center', 'Upper East Side South', 'Penn Station']
    zones = ['Zone 1', 'Zone 2', 'Zone 3', 'Zone 4', 'Zone 5'] 
    
    # Παράδειγμα: [1500000.50, 1200000.20, 950000.00, 800000.00, 750000.00]
    revenues = [500000, 450000, 400000, 350000, 300000] 
    # -------------------------------------------------------

    # Δημιουργία γραφήματος
    plt.figure(figsize=(10, 6))
    bars = plt.bar(zones, revenues, color='#1f77b4', edgecolor='black')

    # Τίτλοι και ετικέτες
    plt.title('Top 5 Taxi Zones by Total Revenue (2024)', fontsize=14, fontweight='bold')
    plt.xlabel('Pickup Zone', fontsize=12)
    plt.ylabel('Total Revenue ($)', fontsize=12)
    
    # Περιστροφή των ονομάτων στον άξονα Χ για να διαβάζονται εύκολα
    plt.xticks(rotation=25, ha='right', fontsize=10)
    
    # Προσθήκη των ποσών πάνω από κάθε μπάρα για καλύτερη παρουσίαση
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + (max(revenues)*0.01), 
                 f'${yval:,.0f}', ha='center', va='bottom', fontsize=10)

    plt.tight_layout()

    # Εξασφάλιση ότι υπάρχει ο φάκελος
    os.makedirs('results/metrics', exist_ok=True)
    
    # Αποθήκευση της εικόνας
    save_path = 'results/metrics/q3_revenue_chart.png'
    plt.savefig(save_path, dpi=300)
    print(f"Το διάγραμμα αποθηκεύτηκε επιτυχώς στο: {save_path}")

if __name__ == "__main__":
    create_bar_chart()