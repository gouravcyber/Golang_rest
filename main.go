package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"io"
//	"reflect"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

type employee struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Balance string `json:"balance"`
}

 var db *sql.DB
 var err error

func initDB() {
	db, err = sql.Open("mysql", "root:Gd#2m@2001@tcp(localhost:3306)/test_db")
	if err != nil {
		fmt.Println("db error", err.Error())
	}
//	defer db.Close()
}
func get_all_employees(w http.ResponseWriter, r *http.Request) {
	// db,err := sql.Open("mysql","root:Gd#2m@2001@tcp(localhost:3306)/test_db")
	// if err !=nil{
	// 	fmt.Println("db error",err.Error())
    // }
	// defer db.Close()
	initDB()
	w.Header().Set("Content-Type", "application/json")
	var employees []employee
	fmt.Println("sqldb", db)
	result, err := db.Query("SELECT * FROM Test")
	if err != nil {
		fmt.Println("error in query", err.Error())
	}
	defer result.Close()
	for result.Next() {
		var employee employee
		err := result.Scan(&employee.ID, &employee.Name, &employee.Balance)
		if err != nil {
			fmt.Println("error in scan", err.Error())
		}
		employees = append(employees, employee)

	}
	json.NewEncoder(w).Encode(employees)
}

func postEmployee(w http.ResponseWriter,r *http.Request){
	initDB()
	result,err:=db.Prepare("INSERT INTO Test(ID,Name,Balance) VALUES(?,?,?)")
	if err!= nil {
		fmt.Println("error in statement prepare", err.Error())
	}
	body,err := io.ReadAll(r.Body)
	if err!= nil {
		fmt.Println("error in reading response body", err.Error())
	}
	keyVal:=make(map[string]string)
	json.Unmarshal(body,&keyVal)
	ID := keyVal["id"]
	Name := keyVal["name"]
    Balance := keyVal["balance"]

	_,err = result.Exec(ID,Name,Balance)
	if err!= nil {
		fmt.Println("error in statement exec", err.Error())
    }
	fmt.Fprint(w,"New employee added successfully.")

}

func getEmployee_byid(w http.ResponseWriter,r *http.Request){
	initDB()
	w.Header().Set("Content-Type", "application/json")
	body,err := io.ReadAll(r.Body)
	if err!=nil{
		fmt.Println("error in reading response body", err.Error())
    }
	keyVal:=make(map[string]string)
	json.Unmarshal(body,&keyVal)
    ID := keyVal["id"]
	result,err := db.Query("SELECT * FROM Test WHERE ID = ?",ID)
	if err!= nil {
		fmt.Println("error in query", err.Error())
    }
	defer result.Close()
	var employee employee
    for result.Next() {
		err =result.Scan(&employee.ID, &employee.Name, &employee.Balance)
		if err != nil{
			fmt.Println("error in scan", err.Error())
        }
		}
	if employee.ID !=""{
		json.NewEncoder(w).Encode(employee)
	}else{
		fmt.Fprintf(w,"Employee with ID =%s does not exists",ID)
	}
    
}

func deleteEmployeebyId(w http.ResponseWriter,r *http.Request){
	initDB()
	body,err := io.ReadAll(r.Body)
	if err!= nil{
		fmt.Println("error in reading response body", err.Error())
    }
	keyVal:=make(map[string]string)
	json.Unmarshal(body,&keyVal)
    ID := keyVal["id"]
	result_f,err := db.Query("SELECT * FROM Test WHERE ID = ?",ID)
	if err !=nil{
		fmt.Println("error in fetching query",err.Error())
	}
	defer result_f.Close()
	var employee employee
    for result_f.Next() {
		err =result_f.Scan(&employee.ID, &employee.Name, &employee.Balance)
		if err!= nil{
			fmt.Println("error in scanning",err.Error())
		}
	}
	if employee.ID !=""{
		result,err := db. Prepare("DELETE FROM Test WHERE ID=?")
		if err!= nil {
			fmt.Println("error in statement prepare", err.Error())
    	}
		_,err = result.Exec(ID)
    	if err!= nil {
			fmt.Println("error in statement execute",err.Error())
		}
		fmt.Fprintf(w,"Details successfully deleted")
	}else{
		fmt.Fprintf(w,"Employee with ID =%s does not exists",ID)
	}
	
}

func updateEmployee(w http.ResponseWriter, r *http.Request){
	initDB()
	body,err := io.ReadAll(r.Body)
	if err!= nil{
		fmt.Println("error  in reading response body",err.Error())
	}
	keyVal:= make(map[string]string)
	json.Unmarshal(body,&keyVal)
	ID := keyVal["id"]
    Balance := keyVal["balance"]
	result_f,err := db.Query("SELECT * FROM Test WHERE ID = ?",ID)
	if err !=nil{
		fmt.Println("error in fetching query",err.Error())
	}
	defer result_f.Close()
	var employee employee
    for result_f.Next() {
		err =result_f.Scan(&employee.ID, &employee.Name, &employee.Balance)
		if err!= nil{
			fmt.Println("error in scanning",err.Error())
		}
	}
	if employee.ID !=""{
		result,err := db.Prepare("UPDATE Test SET Name =? ,Balance =? WHERE ID =?")
		if err!= nil{
			fmt.Println("Error in statement prepare",err.Error())
		}
		_,err = result.Exec(employee.Name,Balance,ID)
		if err !=nil{
			fmt.Println("Error in statement execute",err.Error())
		}
		fmt.Fprintf(w,"Employee details with ID =%s and Name =%s was updated successfully",ID,employee.Name)
	}else
	{
		fmt.Fprintf(w,"Employee with ID =%s does not exists",ID)
    }
}



func main() {
	r := mux.NewRouter()
	r.HandleFunc("/get_all_employees", get_all_employees).Methods("GET")
	r.HandleFunc("/post_employee",postEmployee).Methods("POST")
	r.HandleFunc("/get_employee_byid",getEmployee_byid).Methods("GET")
	r.HandleFunc("/delete_employee_byid",deleteEmployeebyId).Methods("DELETE")
	r.HandleFunc("/update_employee_by_id",updateEmployee).Methods("PUT")
	fmt.Println("Server started")
	log.Fatal(http.ListenAndServe(":6000", r))
}
