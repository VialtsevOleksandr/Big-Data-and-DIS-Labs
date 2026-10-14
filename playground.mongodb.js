/* global use, db */
use('hr_task');

// Запит №1: Забрати резюме користувача за логіном 'oleksandr_kovalenko'
db.getCollection('users').find(
    { login: 'oleksandr_kovalenko' },
    { _id: 0, password_hash: 0 }
);

// Запит №2: Забрати всі унікальні хобі, які зустрічаються серед усіх резюме.
db.getCollection('users').distinct("resume.hobbies");

// Запит №3: Забрати всі унікальні міста, що зустрічаються серед усіх резюме.
db.getCollection('users').distinct("resume.city");

// Запит №4: Забрати всі унікальні хобі мешканців Києва.
db.getCollection('users').distinct("resume.hobbies", { "resume.city": "Київ" });

// Запит №5: Здобувачі зі спільним закладом роботи (групи за компаніями)
db.getCollection('users').aggregate([
    // 1. Розгортаємо масив досвіду на окремі документи
    { $unwind: "$resume.experience" },
    // 2. Групуємо за назвою компанії та збираємо унікальних кандидатів
    {
        $group: {
            _id: "$resume.experience.company",
            colleagues: {
                $addToSet: { $concat: ["$login", " (", "$resume.last_name", " ", "$resume.first_name", ")"] }
            }
        }
    },
    // 3. Залишаємо лише ті компанії, де працювало більше 1 людини (HAVING count > 1)
    {
        $match: {
            $expr: { $gt: [{ $size: "$colleagues" }, 1] }
        }
    },
    // 4. Форматуємо результат
    {
        $project: {
            _id: 0,
            company: "$_id",
            colleagues: 1
        }
    },
    // 5. Сортуємо за алфавітом компаній
    { $sort: { company: 1 } }
]);