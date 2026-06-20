import matplotlib.pyplot as plt
import os

def create_card_share_chart():
    # Ώρες της ημέρας (0 έως 23)
    hours = list(range(24))
    
    # Τα ακριβή δεδομένα 'card_share' από τα αποτελέσματά σου
    card_shares = [
        0.8760, 0.8797, 0.8777, 0.8586, 0.8131, 0.8071, 0.8336, 0.8626, 
        0.8682, 0.8446, 0.8269, 0.8235, 0.8226, 0.8221, 0.8253, 0.8304, 
        0.8405, 0.8574, 0.8658, 0.8694, 0.8722, 0.8772, 0.8798, 0.8751
    ]

    # Μετατροπή σε ποσοστά (επί 100) για καλύτερη απεικόνιση
    card_shares_pct = [x * 100 for x in card_shares]

    plt.figure(figsize=(12, 6))
    
    # Δημιουργία γραμμής με τελείες (markers) σε κάθε ώρα
    plt.plot(hours, card_shares_pct, marker='o', linestyle='-', color='#d62728', linewidth=2, markersize=6)

    # Τίτλοι και άξονες
    plt.title('Ποσοστό Πληρωμών με Κάρτα (Card Share) ανά Ώρα της Ημέρας', fontsize=14, fontweight='bold')
    plt.xlabel('Ώρα της Ημέρας (0-23)', fontsize=12)
    plt.ylabel('Ποσοστό Χρήσης Κάρτας (%)', fontsize=12)
    
    # Ρύθμιση του άξονα Χ για να δείχνει όλες τις ώρες
    plt.xticks(hours)
    
    # Προσθήκη πλέγματος (grid) για να διαβάζεται πιο εύκολα
    plt.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()

    # Αποθήκευση εικόνας
    os.makedirs('results/metrics', exist_ok=True)
    save_path = 'results/metrics/q4_card_share_chart.png'
    plt.savefig(save_path, dpi=300)
    print(f"Το διάγραμμα αποθηκεύτηκε επιτυχώς στο: {save_path}")

if __name__ == "__main__":
    create_card_share_chart()