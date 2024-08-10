package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var DBConn *sql.DB

type User struct {
	id   int
	name string
}

type Seat struct {
	Id     int
	Name   string
	TripId int
	UserId any
}

func bookSeat(user User) (*Seat, error) {
	txn, _ := DBConn.Begin()

	// // APPROACH - 1 => naive approach, not able to allocate all the users to all the seats
	// row := txn.QueryRow("SELECT * FROM seats WHERE trip_id=1 and user_id IS null ORDER BY id LIMIT 1")

	// // APPROACH - 2 => Passimistic (EXCLUSIVE) Locking approach, now able to allocate all the users to all the seats
	// row := txn.QueryRow("SELECT * FROM seats WHERE trip_id=1 and user_id IS null ORDER BY id LIMIT 1 FOR UPDATE")

	// APPROACH - 3 => SKIP locked rows approach, now able to allocate all the users to all the seats and also optimized approach
	row := txn.QueryRow("SELECT * FROM seats WHERE trip_id=1 and user_id IS null ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED")
	if row.Err() != nil {
		return nil, row.Err()
	}

	var seat Seat

	scanerr := row.Scan(&seat.Id, &seat.Name, &seat.TripId, &seat.UserId)
	if scanerr != nil {
		fmt.Printf("ERROR Scan : %v", scanerr.Error())
	}

	_, updateerr := txn.Exec("UPDATE seats SET user_id=? WHERE id=?", user.id, seat.Id)
	if updateerr != nil {
		return nil, updateerr
	}

	commiterr := txn.Commit()
	if commiterr != nil {
		return nil, commiterr
	}

	return &seat, nil
}

func init() {
	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/airline")
	if err != nil {
		log.Fatal(err)
	}
	DBConn = db
}

func getAllUsers() ([]User, error) {
	var users []User

	rows, execerr := DBConn.Query("SELECT * FROM users")
	if execerr != nil {
		log.Fatal(execerr)
	}

	defer rows.Close()

	for rows.Next() {
		var user User
		scanerr := rows.Scan(&user.id, &user.name)
		if scanerr != nil {
			return nil, scanerr
		}
		users = append(users, user)
	}

	return users, nil
}

func main() {
	users, err := getAllUsers()
	if err != nil {
		log.Fatal(err)
	}

	startTime := time.Now()

	var wg sync.WaitGroup

	wg.Add(len(users))

	for _, user := range users {
		go func(user *User) {
			defer wg.Done()
			// book user
			seat, bookerr := bookSeat(*user)
			if bookerr != nil {
				log.Println(bookerr)
			}

			fmt.Printf("\n%s seat assigned to %s [[%d]]", seat.Name, user.name, user.id)
		}(&user)
	}

	wg.Wait()

	fmt.Printf("\nTotal Time taken: %s\n", time.Since(startTime))
}
