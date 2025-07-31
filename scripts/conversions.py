"""
Author: Thomas Kulch
DS5110 - Final Project -  Golf Course Manager
Conversions module
"""
# convert farenheit to celsius
def f_to_c(temp_f):
    return (temp_f - 32) * 5/9

# convert celsius to f
def c_to_f(temp_c):
    return (temp_c * 9/5) + 32

# kilometers to miles
def k_to_m(k):
    return k * 0.621371

if __name__ == '__main__':
    print(f_to_c(56))
    print(f_to_c(95))
    print(c_to_f(5))
    print(c_to_f(0))