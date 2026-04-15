from teleutils.preprocessing.number_format import normalize_number

numbers = [
    "550321999085991755595",
    "550321999061991210058",
    "550321999087991313325",
    "+541152357156",
]

if __name__ == "__main__":
    for number in numbers:
        result, is_valid = normalize_number(number)
        print(f"Input: {number} -> Normalized: {result}, Valid: {is_valid}")
