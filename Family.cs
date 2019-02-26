using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosDB
{
    public class Parent
    {
        public string FamilyName { get; set; }
        public string FirstName { get; set; }
    }

    public class Pet
    {
        public string GivenName { get; set; }
    }

    public class Child
    {
        public string FamilyName { get; set; }
        public string FirstName { get; set; }
        public string Gender { get; set; }
        public int Grade { get; set; }
        public List<Pet> Pets { get; set; }
    }

    public class Address
    {
        public string State { get; set; }
        public string County { get; set; }
        public string City { get; set; }
        public int ZipCode { get; set; }
    }

    public class Family : Resource 
    {
        public string Id { get; set; }
        public string LastName { get; set; }
        public List<Parent> parents { get; set; }
        public List<Child> children { get; set; }
        public Address Address { get; set; }
        public bool IsRegistered { get; set; }
    }
}
