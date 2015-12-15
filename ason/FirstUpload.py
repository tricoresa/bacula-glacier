import boto3
import argparse

# Main loop


def main():

    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="File to upload", required=True)
    parser.add_argument("--account", help="Account ID", required=True)
    parser.add_argument("--vault", help="Vault Name", required=True)
    args = parser.parse_args()

    in_file = open(args.file, "rb")

    glacier = boto3.resource('glacier')
    vault = glacier.Vault(args.account, args.vault)
    archive = vault.upload_archive(
        archiveDescription=args.file,
        body=in_file.read()
    )

    print("Account ID: " + archive.account_id)
    print("Vault name: " + archive.vault_name)
    print("ID: " + archive.id)


if __name__ == "__main__":
    main()

