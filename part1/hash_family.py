import random


def is_prime(n):
    if n == 2 or n == 3: return True
    if n % 2 == 0 or n < 2: return False
    for i in range(3, int(n**0.5)+1, 2):
        if n % i == 0:
            return False
    return True


class UniversalStringHashFamily:
    def __init__(self, number_of_hash_functions, number_of_buckets, min_value_for_prime_number=2,
                 bucket_value_offset=0):
        self.number_of_buckets = number_of_buckets
        self.bucket_value_offset = bucket_value_offset

        primes = []
        number_to_check = max(min_value_for_prime_number, number_of_buckets)
        while len(primes) < number_of_hash_functions:
            if is_prime(number_to_check):
                primes.append(number_to_check)
            number_to_check += random.randint(1, 1000)

        self.hash_function_attrs = []
        for i in range(number_of_hash_functions):
            p = primes[i]
            a = random.randint(1, p)
            a2 = random.randint(1, p)
            b = random.randint(0, p)
            self.hash_function_attrs.append((a, b, p, a2))

    def hash_int(self, int_to_hash, a, b, p):
        return (((a * int_to_hash + b) % p) % self.number_of_buckets) + self.bucket_value_offset

    def hash_str(self, str_to_hash, a, b, p, a2):
        str_to_hash = "1" + str_to_hash  # this will ensure that universality is not affected, see wikipedia for more detail
        l = len(str_to_hash) - 1

        int_to_hash = 0
        for i in range(l + 1):
            int_to_hash += ord(str_to_hash[i]) * (a2 ** (l - i))
        int_to_hash = int_to_hash % p
        return self.hash_int(int_to_hash, a, b, p)

    def __call__(self, function_index, str_to_hash):
        a, b, p, a2 = self.hash_function_attrs[function_index]
        return self.hash_str(str_to_hash, a, b, p, a2)