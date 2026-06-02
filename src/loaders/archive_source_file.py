from pathlib import Path
import shutil


def main():
    landing_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/MeghnaFlow_/Data/Landing')
    archive_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/MeghnaFlow_/Data/Archive')

    csv_files = list(landing_folder.glob("*.csv"))

    if not csv_files:
        print("No files in Landing to archive.")
        return

    for csv_file in csv_files:
        shutil.move(str(csv_file), archive_folder)
        print(f"Archived: {csv_file.name}")

    print(f"All {len(csv_files)} files moved to Archive.")


if __name__ == "__main__":
    main()
