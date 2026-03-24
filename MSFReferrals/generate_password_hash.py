#!/usr/bin/env python3
"""
MSF Referrals Password Hash Generator
Run this script to generate password hashes for new users.
"""

def _simple_hash(s):
    """Helper function for hash extension"""
    h = 0
    for char in s:
        h = ((h << 5) - h) + ord(char)
        if h > 0x7FFFFFFF:
            h = h - 0x100000000
        elif h < -0x80000000:
            h = h + 0x100000000
    
    hex_result = format(h & 0xFFFFFFFF, 'x')
    while len(hex_result) < 8:
        hex_result = '0' + hex_result
    return hex_result[:8]

def hash_password(password):
    """Simple hash function for passwords - matches HTA version exactly"""
    hash_val = 0
    salt = 'michaeli_clinic_2025'
    combined = password + salt
    
    for char in combined:
        hash_val = ((hash_val << 5) - hash_val) + ord(char)
        if hash_val > 0x7FFFFFFF:
            hash_val = hash_val - 0x100000000
        elif hash_val < -0x80000000:
            hash_val = hash_val + 0x100000000
    
    hex_hash = format(hash_val & 0xFFFFFFFF, 'x')
    while len(hex_hash) < 8:
        hex_hash = '0' + hex_hash
    
    extended = hex_hash
    for i in range(7):
        extended += _simple_hash(hex_hash + str(i))
    
    return extended

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        # Password provided as command line argument
        password = sys.argv[1]
    else:
        # Interactive mode
        password = input("Enter password to hash: ")
    
    hash_result = hash_password(password)
    
    print("\n" + "="*70)
    print(f"Password: {password}")
    print(f"Hash:     {hash_result}")
    print("="*70)
    print("\nAdd this to VALID_USERS in MSFReferrals.py:")
    print(f"    'username': '{hash_result}',")
    print()
