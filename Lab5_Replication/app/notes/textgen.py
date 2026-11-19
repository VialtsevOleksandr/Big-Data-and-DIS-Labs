"""Детермінований текст для нотаток: той самий генератор випадкових чисел -> той самий текст."""
from __future__ import annotations

import random
import string

WORDS = (
    "нотатка список покупки зустріч проєкт дедлайн ідея план звіт лекція конспект завдання "
    "нагадування дзвінок лист рахунок квиток подорож рецепт книга фільм музика тренування "
    "бюджет зарплата оренда ремонт подарунок свято робота навчання іспит лабораторна база "
    "реплікація лідер фоловер журнал запис читання затримка мережа вузол кластер сервер "
    "клієнт запит відповідь помилка тест версія реліз код огляд команда спринт задача "
    "пріоритет термін результат висновок питання уточнити перевірити зробити купити "
    "написати надіслати забрати оплатити повторити прочитати обговорити вирішити сьогодні "
    "завтра понеділок вівторок середа четвер п'ятниця вранці ввечері терміново важливо "
    "потім окремо разом швидко уважно молоко хліб кава чай овочі ліки документи паспорт "
    "договір пароль адреса телефон пошта"
).split()

_FILLER = string.ascii_letters + string.digits


def title(rng: random.Random, min_words: int, max_words: int) -> str:
    text = " ".join(rng.choice(WORDS) for _ in range(rng.randint(min_words, max_words)))
    return text[:1].upper() + text[1:]


def body(rng: random.Random, min_chars: int, max_chars: int) -> str:
    """Текст зі слів словника довжиною від min_chars до max_chars символів."""
    target = rng.randint(min_chars, max_chars)
    parts: list[str] = []
    size = 0
    while size < target:
        word = rng.choice(WORDS)
        parts.append(word)
        size += len(word) + 1
    return " ".join(parts)[:target]


def filler(rng: random.Random, n_chars: int) -> str:
    """Випадкові латинські літери й цифри: 1 символ = 1 байт, майже не стискається.

    Використовується в навантаженні, де треба керувати обсягом WAL на один запис.
    """
    return "".join(rng.choices(_FILLER, k=n_chars))
