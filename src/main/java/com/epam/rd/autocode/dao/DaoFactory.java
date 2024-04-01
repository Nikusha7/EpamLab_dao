package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {
    private static ConnectionSource connectionSource;

    public static void initConnectionSource() {
        connectionSource = ConnectionSource.instance();
    }

    public EmployeeDao employeeDAO() {
        final String getByDepartmentQuery = "SELECT * FROM EMPLOYEE where DEPARTMENT=?";

        final String getByManagerQuery = "SELECT * FROM EMPLOYEE where MANAGER=?";

        final String getByID = "SELECT * FROM EMPLOYEE WHERE ID=?";

        final String getAllQuery = "SELECT * FROM EMPLOYEE";

        final String save = "INSERT INTO EMPLOYEE (ID, FIRSTNAME, LASTNAME, MIDDLENAME, POSITION, MANAGER, HIREDATE, SALARY, DEPARTMENT) VALUES (?,?,?,?,?,?,?,?,?)";

        final String delete = "DELETE FROM EMPLOYEE WHERE ID=?";

        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {

                List<Employee> employeesListByDepartment = new ArrayList<>();
                int departmentID = department.getId().intValue();

                initConnectionSource();
                try (final Connection connection = connectionSource.createConnection();
                     PreparedStatement statement = connection.prepareStatement(getByDepartmentQuery)) {

                    statement.setInt(1, departmentID);
                    ResultSet resultSet = statement.executeQuery();

                    while (resultSet.next()) {
                        BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));

                        String firstName = resultSet.getString("FIRSTNAME");
                        String lastName = resultSet.getString("LASTNAME");
                        String middleName = resultSet.getString("MIDDLENAME");
                        FullName fullName = new FullName(firstName, lastName, middleName);

                        Position position = Position.valueOf(resultSet.getString("POSITION"));
                        LocalDate hireDate = resultSet.getDate("HIREDATE").toLocalDate();
                        BigDecimal salary = resultSet.getBigDecimal("SALARY");
                        BigInteger managerId = BigInteger.valueOf(resultSet.getInt("MANAGER"));
                        BigInteger departmentId = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));

                        employeesListByDepartment.add(new Employee(id, fullName, position, hireDate, salary, managerId, departmentId));

                    }

                    resultSet.close();
                    return employeesListByDepartment;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }


            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                List<Employee> employees = new ArrayList<>();
                int managerID = employee.getId().intValue();

                initConnectionSource();

                try (final Connection connection = connectionSource.createConnection();
                     PreparedStatement statement = connection.prepareStatement(getByManagerQuery)) {

                    statement.setInt(1, managerID);
                    ResultSet resultSet = statement.executeQuery();

                    while (resultSet.next()) {
                        BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));

                        String firstName = resultSet.getString("FIRSTNAME");
                        String lastName = resultSet.getString("LASTNAME");
                        String middleName = resultSet.getString("MIDDLENAME");
                        FullName fullName = new FullName(firstName, lastName, middleName);

                        Position position = Position.valueOf(resultSet.getString("POSITION"));
                        LocalDate hireDate = resultSet.getDate("HIREDATE").toLocalDate();
                        BigDecimal salary = resultSet.getBigDecimal("SALARY");
                        BigInteger managerId = BigInteger.valueOf(resultSet.getInt("MANAGER"));
                        BigInteger departmentId = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));

                        employees.add(new Employee(id, fullName, position, hireDate, salary, managerId, departmentId));
                    }

                    resultSet.close();
                    System.out.println(employees);
                    return employees;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                Optional<Employee> employee;
                int employeeID = Id.intValue();

                initConnectionSource();

                try (final Connection connection = connectionSource.createConnection();
                     PreparedStatement statement = connection.prepareStatement(getByID)) {

                    statement.setInt(1, employeeID);
                    ResultSet resultSet = statement.executeQuery();

                    resultSet.next();
                    if (resultSet.getRow() == 1) {
                        BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));

                        String firstName = resultSet.getString("FIRSTNAME");
                        String lastName = resultSet.getString("LASTNAME");
                        String middleName = resultSet.getString("MIDDLENAME");
                        FullName fullName = new FullName(firstName, lastName, middleName);

                        Position position = Position.valueOf(resultSet.getString("POSITION"));
                        LocalDate hireDate = resultSet.getDate("HIREDATE").toLocalDate();
                        BigDecimal salary = resultSet.getBigDecimal("SALARY");
                        BigInteger managerId = BigInteger.valueOf(resultSet.getInt("MANAGER"));
                        BigInteger departmentId = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));

                        resultSet.close();
                        return employee = Optional.of(new Employee(id, fullName, position, hireDate, salary, managerId, departmentId));
                    } else {
                        return Optional.empty();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public List<Employee> getAll() {
                List<Employee> employees = new ArrayList<>();

                initConnectionSource();

                try (final Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {

                    ResultSet resultSet = statement.executeQuery(getAllQuery);

                    while (resultSet.next()) {
                        BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));

                        String firstName = resultSet.getString("FIRSTNAME");
                        String lastName = resultSet.getString("LASTNAME");
                        String middleName = resultSet.getString("MIDDLENAME");
                        FullName fullName = new FullName(firstName, lastName, middleName);

                        Position position = Position.valueOf(resultSet.getString("POSITION"));
                        LocalDate hireDate = resultSet.getDate("HIREDATE").toLocalDate();
                        BigDecimal salary = resultSet.getBigDecimal("SALARY");
                        BigInteger managerId = BigInteger.valueOf(resultSet.getInt("MANAGER"));
                        BigInteger departmentId = BigInteger.valueOf(resultSet.getInt("DEPARTMENT"));

                        employees.add(new Employee(id, fullName, position, hireDate, salary, managerId, departmentId));
                    }

                    resultSet.close();
                    System.out.println(employees);
                    return employees;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Employee save(Employee employee) {
                initConnectionSource();

                try (final Connection connection = connectionSource.createConnection();
                     PreparedStatement statement = connection.prepareStatement(save)) {
//                    ID, FIRSTNAME, LASTNAME, MIDDLENAME, POSITION, MANAGER, HIREDATE, SALARY, DEPARTMENT
                    statement.setInt(1, employee.getId().intValue());
                    statement.setString(2, employee.getFullName().getFirstName());
                    statement.setString(3, employee.getFullName().getLastName());
                    statement.setString(4, employee.getFullName().getMiddleName());
                    statement.setString(5, employee.getPosition().toString());
                    statement.setInt(6, employee.getManagerId().intValue());
                    statement.setDate(7, Date.valueOf(employee.getHired()));
                    statement.setInt(8, employee.getSalary().intValue());
                    statement.setInt(9, employee.getDepartmentId().intValue());

                    int rowsAffected = statement.executeUpdate();

                    return employee;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void delete(Employee employee) {
                initConnectionSource();
                int employeeID = employee.getId().intValue();
                try (final Connection connection = connectionSource.createConnection();
                     PreparedStatement preparedStatement = connection.prepareStatement(delete)) {
                    preparedStatement.setInt(1, employeeID);
                    int rowsAffected = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        final String getByID = "SELECT * FROM DEPARTMENT WHERE ID=?";

        final String getAll = "SELECT * FROM DEPARTMENT";

        final String saveDep = "INSERT INTO DEPARTMENT (ID, NAME, LOCATION) VALUES(?,?,?)";

        final String deleteDep = "DELETE FROM DEPARTMENT WHERE ID=?";
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                initConnectionSource();
                try (final Connection connection = connectionSource.createConnection();
                     PreparedStatement statement = connection.prepareStatement(getByID)) {

                    statement.setInt(1, Id.intValue());

                    ResultSet resultSet = statement.executeQuery();

                    resultSet.next();
                    if (resultSet.getRow() == 1) {

                        BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));
                        String name = resultSet.getString("NAME");
                        String location = resultSet.getString("LOCATION");

                        return Optional.of(new Department(id, name, location));
                    } else {
                        return Optional.empty();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public List<Department> getAll() {
                List<Department> departments = new ArrayList<>();
                initConnectionSource();
                try (final Connection connection = connectionSource.createConnection();
                     Statement statement = connection.createStatement()) {

                    ResultSet resultSet = statement.executeQuery(getAll);

                    while (resultSet.next()) {
                        BigInteger id = BigInteger.valueOf(resultSet.getInt("ID"));
                        String name = resultSet.getString("NAME");
                        String location = resultSet.getString("LOCATION");

                        departments.add(new Department(id, name, location));
                    }

                    return departments;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Department save(Department department) {
                initConnectionSource();

                Optional<Department> alreadyExistedDep = getById(department.getId());
//              we need to delete already existed department because tests are trying to override other dep with same ID,
//              and it can't happen because of primary key constraint. We can not insert Duplicate Primary Key
                alreadyExistedDep.ifPresent(this::delete);

                try (final Connection connection = connectionSource.createConnection();
                     PreparedStatement statement = connection.prepareStatement(saveDep)) {
//                    ID, NAME, LOCATION
                    statement.setInt(1, department.getId().intValue());
                    statement.setString(2, department.getName());
                    statement.setString(3, department.getLocation());

                    int rowsAffected = statement.executeUpdate();

                    return department;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void delete(Department department) {
                initConnectionSource();

                try (final Connection connection = connectionSource.createConnection();
                     PreparedStatement statement = connection.prepareStatement(deleteDep)) {

                    statement.setInt(1, department.getId().intValue());

                    int rowsAffected = statement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }

                ;
    }
}
