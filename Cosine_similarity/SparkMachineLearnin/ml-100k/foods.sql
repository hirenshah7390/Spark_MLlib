
select distinct '1',f.ndb_no,
replace(format(ifnull(calory.calory,0),0),",","") c,
replace(format(ifnull(Protein.Protein,0),0),",","") p,
replace(format(ifnull(ca.ca,0),0),",","") cal,
replace(format(ifnull(glus.glus,0),0),",","") gluc,
replace(format(ifnull(fat.fat,0),0),",","") fat
from fooddescs f
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt)/1000 as calory
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "ENERC_KCAL"
 and w.msre_desc = "cup"
 and amount = 1
)calory on calory.ndb_no = f.ndb_no
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt) as Protein
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "procnt"
 and w.msre_desc = "cup"
 and amount = 1
)protein on protein.ndb_no = f.ndb_no
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt)/1000 as ca
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "ca"
 and w.msre_desc = "cup"
 and amount = 1
)ca on ca.ndb_no = f.ndb_no
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt) as glus
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "glus"
 and w.msre_desc = "cup"
 and amount = 1
)glus on glus.ndb_no = f.ndb_no
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt)/1000 as fat
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "fat"
 and w.msre_desc = "cup"
 and amount = 1
)fat on fat.ndb_no = f.ndb_no
where f.ndb_no not in
(
select ndb_no from
(
select f.ndb_no,
replace(format(ifnull(calory.calory,0),0),",","") c,
replace(format(ifnull(Protein.Protein,0),0),",","") p,
replace(format(ifnull(ca.ca,0),0),",","") cal,
replace(format(ifnull(glus.glus,0),0),",","") gluc,
replace(format(ifnull(fat.fat,0),0),",","") fat from    
fooddescs f
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt)/1000 as calory
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "ENERC_KCAL"
 and w.msre_desc = "cup"
 and amount = 1
)calory on calory.ndb_no = f.ndb_no
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt) as Protein
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "procnt"
 and w.msre_desc = "cup"
 and amount = 1
)protein on protein.ndb_no = f.ndb_no
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt)/1000 as ca
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "ca"
 and w.msre_desc = "cup"
 and amount = 1
)ca on ca.ndb_no = f.ndb_no
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt) as glus
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "glus"
 and w.msre_desc = "cup"
 and amount = 1
)glus on glus.ndb_no = f.ndb_no
left join
(
 select n.ndb_no,(n.nutr_val * w.gm_wgt)/1000 as fat
 from
 nutrientdata n 
 inner join weights w on w.ndb_no = n.ndb_no
 inner join nutrientdefs nd on nd.nutr_no = n.nutr_no
 where 
 nd.tagname = "fat"
 and w.msre_desc = "cup"
 and amount = 1
)fat on fat.ndb_no = f.ndb_no
)a
where a.c = 0 and a.p = 0 and a.cal = 0 and a.gluc = 0 and a.fat=0
 )

