/*
Задание 3.
Найдите топ-3 заведения, где чаще всего менялся менеджер за весь период.*/

SELECT 
    r.cafe_name AS cafe_name, 
    COUNT(m.manager) AS changes_of_managers
FROM cafe.restaurants AS r 
JOIN cafe.restaurant_manager_work_dates AS rmwd ON rmwd.restaurant_uuid = r.restaurant_uuid
JOIN cafe.managers AS m ON m.manager_uuid = rmwd.manager_uuid
GROUP BY cafe_name
ORDER BY changes_of_managers DESC;