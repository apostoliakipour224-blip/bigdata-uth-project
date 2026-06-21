import matplotlib.pyplot as plt
import os

def create_flows_chart():
    # Ρεαλιστικά δεδομένα κορυφαίων ροών (θα τα ενημερώσεις με τις δικές σου τιμές)
    routes = [
        "Manhattan -> Manhattan",
        "Manhattan -> Queens",
        "Queens -> Manhattan",
        "Manhattan -> Brooklyn",
        "Brooklyn -> Manhattan",
        "Queens -> Queens"
    ]
    # Υποθετικά νούμερα διαδρομών βάσει συνήθους κατανομής της Νέας Υόρκης
    trips = [450000, 85000, 72000, 65000, 58000, 42000]

    plt.figure(figsize=(12, 6))
    plt.barh(routes, trips, color='#2ca02c', edgecolor='black', alpha=0.8)
    
    plt.title('Κορυφαίες Γεωγραφικές Ροές Διαδρομών (Borough-to-Borough)', fontsize=14, fontweight='bold')
    plt.xlabel('Αριθμός Διαδρομών (Trips)', fontsize=12)
    plt.ylabel('Ροή (Από -> Προς)', fontsize=12)
    
    plt.gca().invert_yaxis() # Εμφάνιση της μεγαλύτερης ροής στην κορυφή
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    
    plt.tight_layout()
    
    os.makedirs('results/metrics', exist_ok=True)
    save_path = 'results/metrics/q5_borough_flows_chart.png'
    plt.savefig(save_path, dpi=300)
    print(f"Το διάγραμμα ροών αποθηκεύτηκε στο: {save_path}")

if __name__ == "__main__":
    create_flows_chart()