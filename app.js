const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Client } = require('pg');

const app = express();
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));

async function get_restult(client,email,phoneNumber){
    var return_object = {
		"contact":{
			"primaryContatctId": 0,
			"emails": [], 
			"phoneNumbers": [], 
			"secondaryContactIds": []
		}
    };
    // await client.connect()

    var exists = await client.query(`select * from  Contact where email = '${email}' or phonenumber = '${phoneNumber}'`);
        // console.log(exists);
        const exists_rows = exists.rows;
        for(let i =0; i< exists_rows.length; i++){
            if(exists_rows[i].linkprecedence == 'primary'){
                return_object.contact.primaryContatctId = exists_rows[i].id;
            }
            else{
            return_object.contact.secondaryContactIds.push(exists_rows[i].id);
            }
            if(return_object.contact.emails.indexOf(exists_rows[i].email)==-1){
                return_object.contact.emails.push(exists_rows[i].email);
            }
            if(return_object.contact.phoneNumbers.indexOf(exists_rows[i].phonenumber)==-1){
                return_object.contact.phoneNumbers.push(exists_rows[i].phonenumber);
            }
            
        }
        return return_object;

}
app.post('/identify', async function (req, res) {
    // console.log(req);
    let phoneNumber = req.body.phoneNumber;
    let email = req.body.email;
    let linkedId =null;
    let linkPrecedence = 'primary';
    // let createdAt = new Date.now();
    // let updatedAt = new Date.now();
    let deletedAt = null;
    


    const client = new Client({
        user: 'odoo',
        host: 'localhost',
        database: 'practice',
        password: '123456',
        port: 5432,
    })

    await client.connect()
    console.log(req.body);
    var same = await client.query(`select id from  Contact where email = '${email}' and phonenumber = '${phoneNumber}'`);
    // console.log(same); 
    if(same == null) {
        var result = await client.query(`select id from  Contact where email = '${email}' or phonenumber = '${phoneNumber}'`);
    // console.log(result);
    if(result != null ) {
        if(result.rows.length==1){
        linkedId = result.rows[0].id;
        linkPrecedence = 'secondary';
        // console.log(linkedId);
        console.log(await client.query(`INSERT INTO Contact (phonenumber, email, linkedid, linkprecedence, createdat, updatedat, deletedat)
        VALUES ('${phoneNumber}', '${email}', ${linkedId}, '${linkPrecedence}', NOW(), NOW(), ${deletedAt})`));
        // console.log(result);
        return_object = await get_restult(client,email,phoneNumber)
    }
}
    else{
        console.log(await client.query(`INSERT INTO Contact (phonenumber, email, linkedid, linkprecedence, createdat, updatedat, deletedat)
        VALUES ('${phoneNumber}', '${email}', ${linkedId}, '${linkPrecedence}', NOW(), NOW(), ${deletedAt})`));
        return_object = await get_restult(client,email,phoneNumber)
    }
    }
    else{
        return_object = await get_restult(client,email,phoneNumber)
        console.log(return_object);
    }

    await client.end()
    res.send(return_object)
});
app.listen(8884, function () {
    console.log('server started');
});
// CREATE DATABASE Contact
// 5432
// http://localhost:8888/identify