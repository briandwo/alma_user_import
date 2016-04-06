#!/usr/bin/perl

# mdm_to_alma.pl Written by Margaret Briand Wolfe August 2012 for BC Libraries.
# Read current patron information from ODS and build XML file to send to Alma.
# All new patrons get the Patron role automatically assigned by Alma. See User Management, Role Assignment Rules
#
# MBW - ported code to use v. 2 of XSD 6/11/2015
#
eval 'use DBI;';
if ($@)
{
  die "$ENV{AERROR}:module DBI.pm is missing\n";
}

use Net::SFTP::Foreign;    # Secure FTP
use IO::Pty;

$user="bcuser";
$pw="bcpassword";
$sid="dbi:Oracle:DATABASENAMEHERE"; 
%attr = (
   PrintError => 0,
   RaiseError => 0
   );

$dbh=DBI->connect($sid,$user,$pw,\%attr);
if ($DBI::errstr)
{
    die "$ENV{AERROR}:$DBI::errstr\n";
}

($my_day, $my_mon, $my_year) = (localtime) [3,4,5];
$my_year += 1900;
$my_mon += 1;

$my_date = sprintf("%s%02d%02d", $my_year, $my_mon, $my_day);

$last_match_id = '0';

#Open output XML file
$out_fn = sprintf ("%s%s%s", "mdm_patrons_", $my_date, ".xml");
$zip_fn = sprintf ("%s%s%s", "mdm_patrons_", $my_date, ".zip");

$ret = open(OUT_FN, ">$out_fn");
if ($ret < 1)
{
     die ("Cannot open output file $out_fn");
     
}

print OUT_FN ("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
print OUT_FN ("<users>\n");

#Grab active patron data 
$select = <<END;
select
 demo_eagle eagle_id,
 demo_username,
 demo_fname,
 demo_mname,
 demo_lname,
 emp_status status,
 user_affiliation_code,
 value mag_strip
from somedb.someview_person_demographic, somedb.someview_org_employee, somedb.someview_iam_user_affiln, somedb.someview_mag
where demo_eagle = emp_eagle and
 emp_status in ('A', 'P', 'L', 'S', 'W') and
 user_eagle = demo_eagle and 
 id = demo_eagle 
union
select
 demo_eagle eagle_id,
 demo_username,
 demo_fname,
 demo_mname,
 demo_lname,
 stst_status status,
 user_affiliation_code,
 value mag_strip
from somedb.someview_person_demographic, somedb.someview_student_status,  somedb.someview_iam_user_affiln, somedb.someview_mag
where demo_eagle = stst_eagle and
 ((stst_status in ('1', '2', '4', '5', 'J', 'S') and stst_regist_status is not null) or (stst_status is null and stst_regist_status is not null)) and
 user_eagle = demo_eagle and 
 id = demo_eagle
union
select
demo_eagle eagle_id,
demo_username,
demo_fname,
demo_mname,
demo_lname,
'A',
user_affiliation_code,
value mag_strip
from somedb.someview_person_demographic, somedb.someview_mag, somedb.someview_iam_user_affiln
where demo_eagle = id and
demo_eagle = user_eagle and
user_affiliation_code in ('visitscholar', 'visitfaculty', 'stjohnssem', 'stjohnstine', 'contractornt', 'specialstu', 'jesuitassn', 'jesuitcom')
union
select
 demo_eagle eagle_id,
 demo_username,
 demo_fname,
 demo_mname,
 demo_lname,
 'A',
 'Guest',
 value mag_strip
from somedb.someview_person_demographic, somedb.someview_iam_info, somedb.someview_mag
where demo_eagle = iam_eagle and
 iam_other_primary_affiliation = 'spouse' and
 id = demo_eagle and 
 to_char(iam_other_expiration_dt, 'yyyymmdd') >= '$my_date'
order by eagle_id
END
    ;

$sth = $dbh->prepare($select) || die "Failed to prepare query: $dbh->errstr";

$rv = $sth->execute || die "Failed to execute query: $dbh->errstr";

while (($match_id, $lc_username, $first_name, $middle_name, $last_name, $user_status, $user_affil, $mag_id) = $sth->fetchrow_array) 
{

     #If this is the same patron then skip it
     if ($last_match_id eq $match_id)
     {
	  next;
     }

     $skip_rec = $visitor = $visitor_exp_date = $exp_date = 0;

     #Check expiration date for guest (spouse), non-faculty, non-staff and st johns because they don't have an active user status that can be checked
     if ($user_affil eq 'Guest' || $user_affil eq 'visitscholar' || $user_affil eq 'visitfaculty' || $user_affil eq 'stjohnssem' || $user_affil eq 'stjohnstine' || $user_affil eq 'contractornt' || $user_affil eq 'specialstu')
     {

$exp_sel = <<END;
          select
             max(to_char(iam_other_expiration_dt, 'yyyymmdd')),
             iam_other_primary_affiliation
          from somedb.someview_iam_info
          where iam_eagle = '$match_id'
          group by iam_other_expiration_dt,iam_other_primary_affiliation
END
    ;

          $sth1 = $dbh->prepare($exp_sel) || die "Failed to prepare query: $dbh->errstr";

          $rv = $sth1->execute || die "Failed to execute query: $dbh->errstr";

          while ( ($expire_date, $prim_affil) = $sth1->fetchrow_array) 
          {
	       $visitor_exp_date = $expire_date;

               if ($prim_affil eq 'visitscholar' || $prim_affil eq 'visitfaculty' || $prim_affil eq 'stjohnssem' || $prim_affil eq 'stjohnstine' || 
                   $prim_affil eq 'contractornt' || $prim_affil eq 'specialstu' || $prim_affil eq 'spouse')
               {
		    $visitor = 1;
                    $user_affil = $prim_affil;

                    if ($expire_date < $my_date)
                    {
		         $skip_rec = 1;

                         #Ensure this person who used to be visiting hasnt been hired as full time faculty now. FT faculty trumps old visitor status.
$fac_recheck = <<END;
select
 demo_eagle eagle_id,
 emp_status status,
 user_affiliation_code
from somedb.someview_person_demographic, somedb.someview_org_employee, somedb.someview_iam_user_affiln
where demo_eagle = '$match_id' and
 demo_eagle = emp_eagle and
 emp_status in ('A', 'P', 'L', 'S', 'W') and
 user_affiliation_code = 'faculty' and 
 user_eagle = demo_eagle
END

                         $sth1 = $dbh->prepare($fac_recheck) || die "Failed to prepare query: $dbh->errstr";

                         $rv = $sth1->execute || die "Failed to execute query: $dbh->errstr";

                         while ( ($fac_eagle, $fac_status, $fac_affil) = $sth1->fetchrow_array) 
                         {
			      $visitor = $skip_rec = 0;
                              $user_affil = $fac_affil;
                         }

                    }
	       }
          }
     }

     $last_match_id = $match_id;

     
     #Set user group based on patron status.
     if (!$visitor)
     {
          ($patron_status, $grad_term_status) = &get_patron_status($match_id);
     }
     else 
     {
	  $exp_date = $visitor_exp_date;
          
          if ($user_affil eq 'visitfaculty' || $user_affil eq 'visitscholar' || $user_affil eq 'contractornt')
          {
	       $patron_status = '06'; #Faculty        #Longergan Fellows show up as visitscholar
          }
          elsif ($user_affil eq 'stjohnssem' || $user_affil eq 'stjohnstine')
          {
	       $patron_status = '03'; #masters
          }
          elsif ($user_affil eq 'specialstu')
          {
	       $patron_status = '09'; #cross registered
          }
	  elsif ($user_affil eq 'spouse')
          {
               $patron_status = '14';  #Guest
          }
          else #default
          {
               $patron_status = '01'; 
          }
     }

     #Do not load BC Alumni records. These are created internally by access services staff and should not be loaded externally
     if ($patron_status eq '10')
     {
	  $skip_rec = 1;
     }

     if ($skip_rec)
     {
	  next;
     }

     #Make the BC Username all uppercase
     $username = uc($lc_username);

     #If not a visitor then need to set expiry date based on patron status. Faculty = today + 400 days, staff, masters, doctoral = today + 155 days, 
     #Undergrads = today + 63 days
     #Viristor records already have their expiration date set from above
     if (!$exp_date)
     {
	  $grad_term = substr($grad_term_status, 0, 5);
          $exp_date = get_expiration_date($patron_status, $grad_term, $username);
     }

     print OUT_FN ("     <user>\n");

     $line_out = sprintf("%s%s%s", "         <recordType desc=\"Public\">", "PUBLIC", "</recordType>");
     print OUT_FN ("$line_out\n");

     #BC Username is the Primary ID
     $username = rtrim($username);
     $line_out = sprintf("%s%s%s", "         <primary_id>", $username, "</primary_id>");
     print OUT_FN ("$line_out\n");

     #Remove invalid XML characters from patron name fields
     $first_name = xtrim($first_name);
     $middle_name = xtrim($middle_name);
     $last_name = xtrim($last_name);

     $line_out = sprintf("%s%s%s", "         <first_name>", $first_name, "</first_name>");
     print OUT_FN ("$line_out\n");
     
     $line_out = sprintf("%s%s%s", "         <middle_name>", $middle_name, "</middle_name>");
     print OUT_FN ("$line_out\n");

     $line_out = sprintf("%s%s%s", "         <last_name>", $last_name, "</last_name>");
     print OUT_FN ("$line_out\n");

     #If this is faculty or staff grab their department and send it to Alma in the Job Description field.
     $department = "";

     if ($user_affil eq 'spouse')
     {
	  $department = 'Spouse';
     }

     if ($patron_status eq '06' || $patron_status eq '07' || $patron_status eq '08')
     {
          $department = get_department($match_id);
          $department = xtrim($department);
     }

     $line_out = sprintf("%s%s", "         <job_category desc=\"\">", "</job_category>");
     print OUT_FN ("$line_out\n");

     $line_out = sprintf("%s%s%s", "         <job_description>", $department, "</job_description>");
     print OUT_FN ("$line_out\n");

     $line_out = sprintf("%s%s", "         <gender desc=\"\">", "</gender>");
     print OUT_FN ("$line_out\n");


     #Print the patron status retrieved above
     $user_group_desc = &get_group_desc($patron_status);

     $line_out = sprintf("%s%s%s", "         <user_group desc=\"$user_group_desc\">", $patron_status, "</user_group>");
     print OUT_FN ("$line_out\n");


     $line_out = sprintf("%s%s", "         <campus_code desc=\"\">", "</campus_code>");
     print OUT_FN ("$line_out\n");

     $line_out = sprintf("%s%s%s", "         <preferred_language desc=\"English\">", "en", "</preferred_language>");
     print OUT_FN ("$line_out\n");

     #Set expiration date in format yyyy-mm-dd
     $exp_yr = substr($exp_date, 0, 4);
     $exp_mo = substr($exp_date, 4, 2);
     $exp_dy = substr($exp_date, 6, 2);
     $expire_date = sprintf("%s%s%s%s%s", $exp_yr, "-", $exp_mo, "-", $exp_dy);
     
     $line_out = sprintf("%s%s%s", "         <expiry_date>", $expire_date, "</expiry_date>");
     print OUT_FN ("$line_out\n");

     #Set purge date to expiration date
     $line_out = sprintf("%s%s%s", "         <purge_date>", $expire_date, "</purge_date>");
     print OUT_FN ("$line_out\n");

     $line_out = sprintf("%s%s%s", "         <account_type desc=\"External\">", "EXTERNAL", "</account_type>");
     print OUT_FN ("$line_out\n");
     $line_out = sprintf("%s%s%s", "         <external_id>", "SIS", "</external_id>");
     print OUT_FN ("$line_out\n");
     $line_out = sprintf("%s%s%s", "         <status desc=\"Active\">", "ACTIVE", "</status>");
     print OUT_FN ("$line_out\n");

     #Load and output patron addresses, phone numbers and emails
     print OUT_FN ("       <contact_info>\n");
     ($str1, $str2, $city, $state, $zip, $email, $phone) = &get_address($match_id, $patron_status);
     print OUT_FN ("       </contact_info>\n");

     #Start of user IDs
     #ID type
     #Use the codes and not the names.
     #00 is System Number
     #01 is Eagle ID
     #02 is BC UserID
     #03 is NOTIS barcode number
     #04 is Social Security number
     #05 is ID Card Mag Stripe

     #Print out the Eagle ID
     print OUT_FN ("   <user_identifiers>\n");

     #Data for first user ID (Eagle ID)
     print OUT_FN ("       <user_identifier segment_type=\"External\">\n");
     print OUT_FN ("       <id_type desc=\"Eagle ID\">01</id_type>\n");
     $line_out = sprintf("%s%s%s", "         <value>", $match_id, "</value>");
     print OUT_FN ("$line_out\n");
     $line_out = sprintf("%s%s%s", "         <status>", "ACTIVE", "</status>");
     print OUT_FN ("$line_out\n");
     print OUT_FN ("       </user_identifier>\n");

     #Data for second user ID (MagStripe)
     #Remove trailing blanks
     $mag_id = rtrim($mag_id);
     print OUT_FN ("       <user_identifier segment_type=\"External\">\n");
     print OUT_FN ("       <id_type desc=\"ID Card Mag Stripe\">05</id_type>\n");
     $line_out = sprintf("%s%s%s", "         <value>", $mag_id, "</value>");
     print OUT_FN ("$line_out\n");
     $line_out = sprintf("%s%s%s", "         <status>", "ACTIVE", "</status>");
     print OUT_FN ("$line_out\n");
     print OUT_FN ("       </user_identifier>\n");

     print OUT_FN ("   </user_identifiers>\n");


     if ($grad_term_status) #Add statistical category of expected graduation date and old patron status
     {
          print OUT_FN ("       <user_statistics>\n");
          print OUT_FN ("         <user_statistic segment_type=\"External\">\n");
          print OUT_FN ("         <statistic_category desc=\"Expected Graduation Term\">STAC_EXPECT_GRAD_TERM</statistic_category>\n");
          $line_out = sprintf("%s%s%s", "         <statistic_note>", $grad_term_status, "</statistic_note>");
          print OUT_FN ("$line_out\n");
          print OUT_FN ("         </user_statistic>\n");
          print OUT_FN ("       </user_statistics>\n");
     }

     print OUT_FN ("     </user>\n");

}

print OUT_FN ("</users>\n");

close (OUT_FILE);

#Zip the file that is in workspace
@ziplist = ("zip", $zip_fn, $out_fn);
$ret = system(@ziplist);

#Copy the zip file onto BC server where Alma will grab it for loading
if ($sftp = Net::SFTP::Foreign->new("someserver.bc.edu", user => 'someuser', password => 'somepassword', port => '22'))
{
     if ($sftp->setcwd("alma/mdm_patrons"))
     {
          $sftp->put($zip_fn) or die "put failed: " . $sftp->error;
     }  
}

undef $sftp;

exit;

#Calculate patron status
sub get_patron_status
{
     ($eagle_id) = @_;

     $pat_stat = '01';

     #Grab user affiliation
$pat_sel = <<END;
select
  user_affiliation_code,
  user_final_term  
from somedb.someview_iam_user_affiln
where user_eagle = '$eagle_id'
order by user_affiliation_code
END
    ;

     $sth1 = $dbh->prepare($pat_sel) || die "Failed to prepare query: $dbh->errstr";

     $rv = $sth1->execute || die "Failed to execute query: $dbh->errstr";

     while (($affil, $final_term) = $sth1->fetchrow_array) 
     {
	  $pat_stat = $affil;
          $grad_sem = $final_term;
     }

     return ($pat_stat, $grad_sem);
}

sub get_group_desc
{
     ($pat_stat) = @_;

     $group_desc = "";

     if ($pat_stat eq '01')
     {
	  $group_desc = "BC Undergraduate";
     }
     elsif ($pat_stat eq '02')
     {
	  $group_desc = "College of Advancing Studies";
     }
     elsif ($pat_stat eq '03')
     {
	  $group_desc = "BC Master\'s";
     }
     elsif ($pat_stat eq '04')
     {
	  $group_desc = "BC Doctoral";
     }
     elsif ($pat_stat eq '04')
     {
	  $group_desc = "BC Doctoral";
     }
     elsif ($pat_stat eq '05')
     {
	  $group_desc = "BC Law Student";
     }
     elsif ($pat_stat eq '06')
     {
	  $group_desc = "BC Faculty";
     }
     elsif ($pat_stat eq '07')
     {
	  $group_desc = "BC Law Faculty";
     }
     elsif ($pat_stat eq '08')
     {
	  $group_desc = "BC Staff";
     }
     elsif ($pat_stat eq '09')
     {
	  $group_desc = "Cross Registered";
     }
     elsif ($pat_stat eq '10')
     {
	  $group_desc = "BC Alumni";
     }
     elsif ($pat_stat eq '14')
     {
	  $group_desc = "Guest";
     }
     elsif ($pat_stat eq '28')
     {
	  $group_desc = "Presidential Scholar";
     }
     elsif ($pat_stat eq '31')
     {
	  $group_desc = "University VIP\/Privileged User";
     }
     elsif ($pat_stat eq '34')
     {
	  $group_desc = "Graduating";
     }
     elsif ($pat_stat eq '36')
     {
	  $group_desc = "Graduating Law";
     }

     return($group_desc);     

}

#Determine expiration date based on patron status
sub get_expiration_date
{
     ($pat_stat, $grad, $bc_name) = @_;

     $my_time = time();

     if ($pat_stat eq '06' || $pat_stat eq '07'|| $pat_stat eq '08')
     {
          #Faculty and staff get 400 days
          $no_days = 400;
     }
     elsif ($pat_stat eq '03' || $pat_stat eq '04' || $pat_stat eq '05' || $pat_stat eq '28' || $pat_stat eq '09')
     {
          #Masters, Doctoral, Cross Registered, Law Students and Presidential Scholars get 155 days
          $no_days = 155;
     }
     else
     {
          #Undergrads, College of Advancing Studies, and everyone else get 63 days
          $no_days = 63;
     }

     $exp_time = $my_time + ($no_days * 24 * 60 * 60);
     ($exp_sec, $exp_min, $exp_hr, $exp_day, $exp_mon, $exp_yr, $exp_dow, $exp_doy, $exp_dls) = localtime($exp_time);
     $exp_yr += 1900;
     $exp_mon += 1;

     $expiry_date = sprintf("%s%02d%02d", $exp_yr, $exp_mon, $exp_day);

     return($expiry_date);     
}

#Determine department affiliated with based on patron status
sub get_department
{
     ($eagle_id) = @_;

$dep_sel = <<END;
select
 status_dept_name
from somedb.someview_org_employee_status
where status_eagle = '$eagle_id'
END
    ;

     $sth1 = $dbh->prepare($dept_sel) || die "Failed to prepare query: $dbh->errstr";

     $rv = $sth1->execute || die "Failed to execute query: $dbh->errstr";

     $save_dept = 'Not found';

     while ($dept = $sth1->fetchrow_array) 
     {
	  $save_dept = $dept;
     }
 
     return($save_dept);     
}

#Get Address, phone and email
sub get_address
{
     ($eagle_id, $pat_stat) = @_;

     my @city_list;
     my $i = 0;
     my $k = 0;
     my $skip_addr = 0;
     my $str1, str2, $cty, $st, $zp, $em, $ph, $have_it;

     $str1 = $str2 = $cty = $st = $zp = $em = $ph = "";

     undef @atype_x;
     undef @street1_x;
     undef @street2_x;
     undef @street3_x;
     undef @street4_x;
     undef @city_x;     
     undef @state_x;     
     undef @zip_x;
     undef @city_list;
     undef @pref_x;
     $aidx = 0;  

     $have_it = 0;

     #Grab address info
$pat_sel = <<END;
select
 addr_line1,
 addr_line2,
 addr_line3,
 addr_last_line,
 addr_city,
 addr_state,
 addr_zip,
 addr_type,
 addr_bc_bldg,
 addr_bc_room
from somedb.someview_person_address
where addr_eagle = '$eagle_id' and
addr_type in ('HOME', 'LOCAL', 'WORKBC')
END
    ;

     $sth1 = $dbh->prepare($pat_sel) || die "Failed to prepare query: $dbh->errstr";

     $rv = $sth1->execute || die "Failed to execute query: $dbh->errstr";

     print OUT_FN ("       <addresses>\n");

     while (($street1, $street2, $street3, $street4, $city, $state, $zip, $atype, $bldg, $room) = $sth1->fetchrow_array) 
     {
          #Check for blank address and skip it if blank
	  $len = length($city);
          if ($len <= 0)
          {
	      next;
          }

          #Store the street address, if it's the same as the last address then skip it as the MDM returns a lot of the same address and we don't need to load all the dups into Alma.
          if ($i == 0)
          {
	       $city_list[$i] = $city;
               $i++;
          }
          else
          {
	       for ($skip_addr = 0, $k = 0; $k < $i; $k++)
               {
                    if ($city eq $city_list[$k])
                    {
			 $skip_addr = 1;
                         $k = $i;
                    }
               }
          }

          #Remove trailing blanks from street1
          #If its blank then skip it. Alma will not load a record without a street 1.
          $street1 = rtrim($street1);
          $str1_len = length($street1);
          if (!$str1_len)
          {
	      $skip_addr = 1;
          }

          #Go to next loop iteration if this street address is a duplicate.
          if ($skip_addr)
          {
	       next;

          }
          elsif ($i != 1)
          {
	       $city_list[$i] = $city;
               $i++;
          }

          #Remove invalid XML characters
          $street1_x[$aidx] = xtrim($street1);

          #Remove trailing blanks
          $street2 = rtrim($street2);
          #Remove invalid XML characters
          $street2_x[$aidx] = xtrim($street2);

          #Remove trailing blanks
          $street3 = rtrim($street3);
          #Remove invalid XML characters
          $street3_x[$aidx] = xtrim($street3);

          #Remove trailing blanks
          $street4 = rtrim($street4);
          #Remove invalid XML characters
          $street4_x[$aidx] = xtrim($street4);

          $city = rtrim($city);
          #Remove invalid XML characters
          $city_x[$aidx] = xtrim($city);

          $state = rtrim($state);
          #Remove invalid XML characters
          $state_x[$aidx] = xtrim($state);

          $zip = rtrim($zip);
          #Remove invalid XML characters
          $zip_x[$aidx] = xtrim($zip);

          $atype_x[$aidx] = $atype;

          if ($atype eq 'WORKBC')  #BC addresses only have a code to facility_site view
          {
               $work_sel = <<END;
select
     site_desc
from somedb.someview_facility_site
where site_code = '$bldg'
END
    ;

               $sth2 = $dbh->prepare($work_sel) || die "Failed to prepare query: $dbh->errstr";

               $rv = $sth2->execute || die "Failed to execute query: $dbh->errstr";

               while ($bc_site = $sth2->fetchrow_array) 
               {
                   if ($room)
                   {
                        $street1_x[$aidx] = sprintf("%s%s%s", $room, " ", $bc_site);
                    }
                   else
                   {
		        $street1_x[$aidx] = $bc_site;
                   }

		   $ret = $bc_site =~ /LAW /;
                   if ($ret)
                   {
		        $city_x[$aidx] = "Newton";
                        $state_x[$aidx] = "MA";
                        $zip_x[$aidx] = "02459";
                   }
                   else
                   {
		        $street2_x[$aidx] = "140 Commonwealth Ave";
		        $city_x[$aidx] = "Chestnut Hill";
                        $state_x[$aidx] = "MA";
                        $zip_x[$aidx] = "02467";
                   }
	       }
	  }

          $aidx++;
     }

     if ($aidx > 0)
     {
          if ($aidx == 1) #If only 1 address then it is the preferred one
          {
	       $pref_x[0] = 'TRUE';
          }    
          else
          {
	       for ($i = 0; $i < $aidx; $i++)
               {
                    if ($atype_x[$i] eq 'WORKBC' || $atype_x[$i] eq 'LOCAL')
                    {
	                 $pref_x[$i] = 'TRUE';
                    }
                    else #Home address
                    {
	                 $pref_x[$i] = 'FALSE';
                    }
               } 
          }      

          #Print them out
          for ($i = 0; $i < $aidx; $i++)
          {
               if ($pref_x[$i] eq 'TRUE')
               {
                    print OUT_FN ("       <address preferred=\"true\" segment_type=\"External\">\n");
               }
               else
               {
                    print OUT_FN ("       <address preferred=\"false\" segment_type=\"External\">\n");
               }

               $line_out = sprintf("%s%s%s", "         <line1>", $street1_x[$i], "</line1>");
               print OUT_FN ("$line_out\n");

               if ($street2_x[$i])
               {
                    $line_out = sprintf("%s%s%s", "         <line2>", $street2_x[$i], "</line2>");
                    print OUT_FN ("$line_out\n");
               }

               if ($street3_x[$i])
               {
                    $line_out = sprintf("%s%s%s", "         <line3>", $street3_x[$i], "</line3>");
                    print OUT_FN ("$line_out\n");
               }

               if ($street4_x[$i])
               {
                    $line_out = sprintf("%s%s%s", "         <line4>", $street4_x[$i], "</line4>");
                    print OUT_FN ("$line_out\n");
               }

               $line_out = sprintf("%s%s%s", "         <city>", $city_x[$i], "</city>");
               print OUT_FN ("$line_out\n");

               $line_out = sprintf("%s%s%s", "         <state_province>", $state_x[$i], "</state_province>");
               print OUT_FN ("$line_out\n");

               $line_out = sprintf("%s%s%s", "         <postal_code>", $zip_x[$i], "</postal_code>");
               print OUT_FN ("$line_out\n");

               #Address start and end dates - how should I calculate these?
               $line_out = sprintf("%s%s%s", "         <start_date>", "2015-06-01", "</start_date>");
               print OUT_FN ("$line_out\n");
               $line_out = sprintf("%s%s%s", "         <end_date>", "2018-08-31", "</end_date>");
               print OUT_FN ("$line_out\n");

               #Address type
               if ($atype_x[$i] eq 'HOME' || $atype_x[$i] eq 'LOCAL')
               {
                    print OUT_FN ("           <address_types><address_type desc=\"Home\">home</address_type>\n");
               }
               elsif ($atype_x[$i] eq 'WORKBC')
               {
                    print OUT_FN ("           <address_types><address_type desc=\"Work\">work</address_type>\n");
               }

               print OUT_FN ("         </address_types>\n");
               print OUT_FN ("       </address>\n");

          }
     }

     print OUT_FN ("       </addresses>\n");

     for ($i = 0; $i < $aidx; $i++)
     {
          #Send a preferred address to ILLIAD
          if ($pref_x[$i])
	  {
	       $str1 = $street1_x[$i];
	       $str2 = $street2_x[$i];
	       $cty = $city_x[$i];
               $st = $state_x[$i];
               $zp = $zip_x[$i];
          }
     }

     $have_it = 0;

     #Add in user email
$em_sel = <<END;
select
    cont_value,
    cont_obj_type,
    cont_type
from somedb.someview_person_contact
where cont_eagle = '$eagle_id' and
cont_obj_type = 'EMAIL' and
cont_type = 'EMAILBC'
END
    ;

     $sth4 = $dbh->prepare($em_sel) || die "Failed to prepare query: $dbh->errstr";

     $rv = $sth4->execute || die "Failed to execute query: $dbh->errstr";

     print OUT_FN ("       <emails>\n");

     while (($c_info, $c_obj, $c_type) = $sth4->fetchrow_array) 
     {
          #Remove trailing blanks
          $c_info = rtrim($c_info);
          #Remove invalid XML characters
          $c_info = xtrim($c_info);
          $len = length($c_info);

          if ($len > 0)
          {
               print OUT_FN ("       <email preferred=\"true\">\n");

               $line_out = sprintf("%s%s%s", "         <email_address>", $c_info, "</email_address>");
               print OUT_FN ("$line_out\n");

               print OUT_FN ("       <email_types>\n");

               print OUT_FN ("           <email_type desc=\"Personal\">personal</email_type>\n");

               print OUT_FN ("       </email_types>\n");
               print OUT_FN ("       </email>\n");

               if (!$have_it)
               {
	            $em = $c_info;
                    $have_it = 1;
               }

          }
     }

     print OUT_FN ("       </emails>\n");

     $have_it = 0;

     #Add in user phone(s)
$contact_sel = <<END;
select
    cont_value,
    cont_obj_type,
    cont_type
from somedb.someview_person_contact
where cont_eagle = '$eagle_id' and
cont_obj_type = 'PHONE' and
cont_type in ('WORKBC', 'HOME', 'LOCAL')
END
    ;

     $sth3 = $dbh->prepare($contact_sel) || die "Failed to prepare query: $dbh->errstr";

     $rv = $sth3->execute || die "Failed to execute query: $dbh->errstr";

     print OUT_FN ("       <phones>\n");

     while (($c_info, $c_obj, $c_type) = $sth3->fetchrow_array) 
     {
          if ($c_type eq "HOME")
          {
               print OUT_FN ("       <phone preferred=\"false\" preferred_sms=\"false\" segment_type=\"External\">\n");
          }
          else
          {
               print OUT_FN ("       <phone preferred=\"true\" preferred_sms=\"false\" segment_type=\"External\">\n");
          }

          #Remove trailing blanks
          $c_info = rtrim($c_info);
          #Remove invalid XML characters
          $c_info = xtrim($c_info);
          $line_out = sprintf("%s%s%s", "         <phone_number>", $c_info, "</phone_number>");
          print OUT_FN ("$line_out\n");
          print OUT_FN ("       <phone_types>\n");

          if ($c_type eq "HOME" || $c_type eq "LOCAL")
          {
               print OUT_FN ("           <phone_type desc=\"Home\">home</phone_type>\n");
          }
          else
          {
               print OUT_FN ("           <phone_type desc=\"Office\">office</phone_type>\n");
          }

          print OUT_FN ("       </phone_types>\n");
          print OUT_FN ("       </phone>\n");

          $len = length($c_info);
          if (!$have_it && $len)
          {
	       $ph = $c_info;
               $have_it = 1;
          }

     }

     print OUT_FN ("       </phones>\n");

     return ($str1, $str2, $cty, $st, $zp, $em, $ph);

}


#Strip the blanks from the right side of the string
sub rtrim($)
{
    my $string = shift;
    $string =~ s/\s+$//;
    return $string;
}

#Escape invalid XML characters or the file will fail to load
sub xtrim($)
{
    my $str = shift;

    #Look for any ampersands and escape it if there are any
    $str =~ s/&/ &amp; /g;

    #Look for a left angle bracket and escape it if there are any
    $str =~ s/</ &lt; /g;

    #Look for a right angle bracket and escape it if there are any
    $str =~ s/>/ &gt; /g;

    #Look for quotation mark and escape it if there are any
    $str =~ s/"/ &quot; /g;

    #Look for apostrophe and escape it if there are any
    $str =~ s/'/ &apos; /g;

    #Get rid of any percent signs (a lot of blank address lines with percent sign only)
    $str =~ s/%//g;

    return $str;
}




