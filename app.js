const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Client } = require('pg');

const app = express();
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));
async function get_result(client,primary_id){
    var return_object = {
        "contact":{
            "primaryContatctId": 0,
            "emails": [], 
            "phoneNumbers": [], 
            "secondaryContactIds": []
        }
    };
    var exists = await client.query(`select email,phonenumber,linkprecedence,id from  Contact where linkedid=${primary_id} or id=${primary_id}`);
        const exists_rows = exists.rows;
        console.log("Exists rows: " );
            console.log(exists_rows);
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
async function createOrUpdateContact(client, id, email, phoneNumber, linkedId, linkPrecedence, insertOpt) {
    console.log(id, email, phoneNumber, linkedId, linkPrecedence, insertOpt);
    
    if (insertOpt === 1) {
        return await client.query(`
            INSERT INTO Contact (phonenumber, email, linkedid, linkprecedence, createdat, updatedat)
            VALUES ($1, $2, $3, $4, NOW(), NOW()) 
            RETURNING id`, [phoneNumber, email, linkedId, linkPrecedence]);
    } else {
        await client.query(`
            UPDATE Contact 
            SET linkprecedence = $1, updatedat = NOW(), linkedid = $2 
            WHERE id = $3`, [linkPrecedence, linkedId, id]);
    }
}
app.post('/identify', async function (req, res) {
    try{
    const phoneNumber = req.body.phoneNumber;
    const email = req.body.email;
    if(phoneNumber==undefined && email==undefined){
         res.status(404).send({"Error":"Please provide the required data email and phoneNumber"});
        return;
    }
    const conString = "postgres://geeri:8VVmTvxWm1S2KPxEDiNgCMIMmM17jOQk@dpg-cj9s8b9duelc739s97q0-a.singapore-postgres.render.com/masterdb_q9tg?ssl=true"
    const client = new Client(conString);
    await client.connect()
    
    let linkedId =null;
    let linkPrecedence = 'primary';
    let return_object;
    let primary_id;
    let containsNull=false;
    if(email ==null || phoneNumber ==null){
        containsNull=true;
    }
    var getAllMatchingData = await client.query(`select * from Contact where email = '${email}' or phonenumber = '${phoneNumber}'`);
    console.log(getAllMatchingData);
    allMatchingData=getAllMatchingData.rows;
    if(containsNull){
        if(allMatchingData.length!=0){
        primary_id=allMatchingData[0].linkedid!=null?allMatchingData[0].linkedid:allMatchingData[0].id;
        return_object = await get_result(client, primary_id);
         res.status(200).send(return_object);
        return;
        }
        else{
            res.status(200).send({"Error":"email / phonenumber should not be null insert the data"});
            return;
        }
    }
    if(allMatchingData.length==0){
        console.log('new Insert');
        let new_data = await createOrUpdateContact(client,null,email, phoneNumber,linkedId,linkPrecedence,1);
        console.log(new_data);
        primary_id = new_data.rows[0].id;
        return_object = await get_result(client, primary_id);
         res.status(200).send(return_object);
        return;
    }
    else {
        
        let exist_email,exist_phonenumber;
        var primaryContacts=[];
        for (let row = 0; row < allMatchingData.length; row++) {
            console.log(allMatchingData[row]);
            if (allMatchingData[row].email == email){
                 exist_email=1;
             }
             if(allMatchingData[row].phonenumber == phoneNumber){
                  exist_phonenumber=1;
             }
          
            if (allMatchingData[row].linkprecedence == 'primary') {
                primary_id=allMatchingData[row].id;
                primaryContacts.push(allMatchingData[row]); 
            }
        }
        //console.log(exist_email,exist_phonenumber);
        if(exist_email && exist_phonenumber && primaryContacts.length == 1){ 
            return_object=await get_result(client, primary_id); 
             res.status(200).send(return_object);
            return;
        }   
        if(primaryContacts.length == 2 ){
           // console.log('update 2 existing')
            let date0=primaryContacts[0].createdat;
            let date1=primaryContacts[1].createdat; 
            let id_parent = date0 > date1 ? primaryContacts[1].id : primaryContacts[0].id;
            let id_child = date0 > date1 ? primaryContacts[0].id : primaryContacts[1].id;
            linkPrecedence = 'secondary';
            primary_id=id_parent;
            let updatedData=createOrUpdateContact(client,id_child,email,phoneNumber,id_parent,linkPrecedence,0);
           // await client.query(`update Contact set linkprecedence = ${linkPrecedence}, updatedat = NOW(), linkedid=${id_parent} where id= ${id_child}`);
            return_object=await get_result(client,primary_id);
        }
        else{
            linkedId = primaryContacts.length == 1 ?primaryContacts[0].id: allMatchingData[0].linkedid;
            linkPrecedence = 'secondary';
            primary_id=linkedId;
            let new_data =createOrUpdateContact(client,null,email, phoneNumber,linkedId,linkPrecedence,1);
            
            return_object = await get_result(client,primary_id);
        }
    }
    
    await client.end();
     res.status(200).send(return_object);
    return;
    }
    catch (error) {
        console.error("An error occurred:", error);
        res.status(500).send({ "error": "An internal server error occurred." });
    }
});
app.listen(8884, function () {
    console.log('server started');
});
