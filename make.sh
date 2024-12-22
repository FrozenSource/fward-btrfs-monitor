pyinstaller \
    --onefile \
    --noconfirm \
    --clean \
    --strip \
    --collect-all apprise \
    --collect-all btrfs \
    fward.py