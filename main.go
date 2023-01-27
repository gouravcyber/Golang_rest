package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
//	"strconv"

	//	"strconv"
	//	"reflect"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

type employee struct {
	ID      int `json:"id"`
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
	result, err := db.Query("SELECT * FROM Test_new")
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
	result,err:=db.Prepare("INSERT INTO Test_new(ID,Name,Balance) VALUES(?,?,?)")
	if err!= nil {
		fmt.Println("error in statement prepare", err.Error())
	}
	body,err := io.ReadAll(r.Body)
	if err!= nil {
		fmt.Println("error in reading response body", err.Error())
	}
//	keyVal:=make(map[string]string)
	var temp_emp employee
	json.Unmarshal(body,&temp_emp)
	ID:= temp_emp.ID
	Name := temp_emp.Name
    Balance := temp_emp.Balance

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
//	keyVal:=make(map[string]string)
	var temp_emp employee
	json.Unmarshal(body,&temp_emp)
    ID := temp_emp.ID
	result,err := db.Query("SELECT * FROM Test_new WHERE ID = ?",ID)
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
	if employee.ID != 0{
		json.NewEncoder(w).Encode(employee)
	}else{
		fmt.Fprintf(w,"Employee with ID =%v does not exists",ID)
	}
    
}

func deleteEmployeebyId(w http.ResponseWriter,r *http.Request){
	initDB()
	body,err := io.ReadAll(r.Body)
	if err!= nil{
		fmt.Println("error in reading response body", err.Error())
    }
//	keyVal:=make(map[string]string)
	var temp_emp employee
	json.Unmarshal(body,&temp_emp)
    ID := temp_emp.ID
	result_f,err := db.Query("SELECT * FROM Test_new WHERE ID = ?",ID)
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
	if employee.ID != 0{
		result,err := db. Prepare("DELETE FROM Test_new WHERE ID=?")
		if err!= nil {
			fmt.Println("error in statement prepare", err.Error())
    	}
		_,err = result.Exec(ID)
    	if err!= nil {
			fmt.Println("error in statement execute",err.Error())
		}
		fmt.Fprintf(w,"Details successfully deleted")
	}else{
		fmt.Fprintf(w,"Employee with ID =%v does not exists",ID)
	}
	
}

func updateEmployee(w http.ResponseWriter, r *http.Request){
	initDB()
	body,err := io.ReadAll(r.Body)
	if err!= nil{
		fmt.Println("error  in reading response body",err.Error())
	}
	var temp_emp employee
//	keyVal:= make(map[string]string)
	json.Unmarshal(body,&temp_emp)
	ID := temp_emp.ID
    Balance := temp_emp.Balance
	result_f,err := db.Query("SELECT * FROM Test_new WHERE ID = ?",ID)
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
	if employee.ID !=0{
		result,err := db.Prepare("UPDATE Test_new SET Name =? ,Balance =? WHERE ID =?")
		if err!= nil{
			fmt.Println("Error in statement prepare",err.Error())
		}
		_,err = result.Exec(employee.Name,Balance,ID)
		if err !=nil{
			fmt.Println("Error in statement execute",err.Error())
		}
		fmt.Fprintf(w,"Employee details with ID =%v and Name =%s was updated successfully",ID,employee.Name)
	}else
	{
		fmt.Fprintf(w,"Employee with ID =%v does not exists",ID)
    }
}

func batch_insertion(w http.ResponseWriter, r *http.Request){
	initDB()
	//var test_slice[]string
	var present_employee []int
	var added_employee []int
	keyVal :=make(map[string][]int)
	body,err :=io.ReadAll(r.Body)
	if err !=nil{
		fmt.Println("Error in reading the response body",err.Error())
	}
	json.Unmarshal(body,&keyVal)
	test_slice := keyVal["id"]
	for i:=0;i<len(test_slice);i++{
		result,err :=db.Query("SELECT * FROM Test_new WHERE ID = ?",test_slice[i])
		if err != nil{
			fmt.Println("error in fetching data",err.Error())
		}
		defer result.Close()
		var employee_n employee
    	for result.Next() {
			err =result.Scan(&employee_n.ID, &employee_n.Name, &employee_n.Balance)
			if err!= nil{
			fmt.Println("error in scanning",err.Error())
			}
		}
			
			if employee_n.ID == 0{
				//fmt.Println("Checking",employee_n.ID)
				result_n,err := db.Prepare("INSERT INTO Test_new(ID) VALUES(?)")
				if err != nil{
					fmt.Println("error in preparing",err.Error())
				}
				_,err = result_n.Exec(test_slice[i])
				if err!=nil{
					fmt.Println("error in executing",err.Error())
				}
				added_employee = append(added_employee, test_slice[i])
			}else{
				present_employee = append(present_employee, employee_n.ID)
			}
		
	}
	fmt.Fprintf(w,"added_employee: %v, present_employee: %v",added_employee,present_employee)
}



func main() {
	r := mux.NewRouter()
	r.HandleFunc("/get_all_employees", get_all_employees).Methods("GET")
	r.HandleFunc("/post_employee",postEmployee).Methods("POST")
	r.HandleFunc("/get_employee_byid",getEmployee_byid).Methods("GET")
	r.HandleFunc("/delete_employee_byid",deleteEmployeebyId).Methods("DELETE")
	r.HandleFunc("/update_employee_by_id",updateEmployee).Methods("PUT")
	r.HandleFunc("/insert_employee_by batch",batch_insertion).Methods("POST")
	fmt.Println("Server started")
	log.Fatal(http.ListenAndServe(":6000", r))
}
