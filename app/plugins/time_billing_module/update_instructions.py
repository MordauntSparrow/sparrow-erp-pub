import install as installer

if __name__ == "__main__":
    # Fresh installs or updates both end up here
    installer.install(seed_demo=False)
    installer.upgrade()
    print("Time & Billing Module installation/upgrade complete.")
